import json
from unittest.mock import AsyncMock, MagicMock

import pytest

from jeopardy.activities import _normalize, judge_answer, persist_result
from jeopardy.models import Clue, Turn


def _tool_use_response(correct: bool, reason: str) -> MagicMock:
    block = MagicMock()
    block.type = "tool_use"
    block.name = "record_judgement"
    block.input = {"correct": correct, "reason": reason}
    response = MagicMock()
    response.content = [block]
    response.stop_reason = "tool_use"
    return response


@pytest.fixture
def mock_anthropic(monkeypatch):
    client = MagicMock()
    client.messages.create = AsyncMock()
    monkeypatch.setattr("jeopardy.activities._anthropic_client", client)
    return client


class TestNormalize:
    def test_lowercases(self):
        assert _normalize("HELLO") == "hello"

    def test_strips_question_prefix(self):
        assert _normalize("What is Paris") == "paris"
        assert _normalize("Who was Lincoln") == "lincoln"
        assert _normalize("where are the Alps") == "alps"

    def test_strips_articles(self):
        assert _normalize("The Eiffel Tower") == "eiffel tower"
        assert _normalize("a horse") == "horse"
        assert _normalize("an apple") == "apple"

    def test_strips_punctuation(self):
        assert _normalize("Mr. Smith!") == "mr smith"
        assert _normalize("hello, world?") == "hello world"

    def test_collapses_whitespace(self):
        assert _normalize("  hello   world  ") == "hello world"

    def test_combined(self):
        assert _normalize("What is the Eiffel Tower?") == "eiffel tower"

    def test_article_not_stripped_when_glued_to_punctuation(self):
        assert _normalize("Who is A. Lincoln?") == "a lincoln"

    def test_question_prefix_only_at_start(self):
        assert _normalize("a what is question") == "what is question"

    def test_empty(self):
        assert _normalize("") == ""
        assert _normalize("   ") == ""


class TestJudgeAnswerFastPath:
    async def test_exact_match_returns_correct_without_llm(self, mock_anthropic):
        result = await judge_answer(Clue(prompt="...", answer="Paris"), "What is Paris?")
        assert result.correct
        assert result.canonical_answer == "Paris"
        assert result.reason is None
        mock_anthropic.messages.create.assert_not_called()

    async def test_canonical_answer_preserves_case(self, mock_anthropic):
        result = await judge_answer(Clue(prompt="...", answer="Eiffel Tower"), "eiffel tower")
        assert result.correct
        assert result.canonical_answer == "Eiffel Tower"
        mock_anthropic.messages.create.assert_not_called()


class TestJudgeAnswerLLMPath:
    async def test_correct_with_reason(self, mock_anthropic):
        mock_anthropic.messages.create.return_value = _tool_use_response(
            True, "Accepts 'Twain' as short for 'Mark Twain'."
        )
        result = await judge_answer(Clue(prompt="...", answer="Mark Twain"), "Twain")
        assert result.correct
        assert result.canonical_answer == "Mark Twain"
        assert result.reason == "Accepts 'Twain' as short for 'Mark Twain'."
        mock_anthropic.messages.create.assert_called_once()

    async def test_incorrect_with_reason(self, mock_anthropic):
        mock_anthropic.messages.create.return_value = _tool_use_response(
            False, "Lincoln was a different president."
        )
        result = await judge_answer(
            Clue(prompt="...", answer="George Washington"), "Lincoln"
        )
        assert not result.correct
        assert result.canonical_answer == "George Washington"
        assert result.reason == "Lincoln was a different president."

    async def test_request_uses_haiku_with_caching_and_forced_tool(self, mock_anthropic):
        mock_anthropic.messages.create.return_value = _tool_use_response(False, "...")
        await judge_answer(Clue(prompt="P", answer="A"), "wrong")

        kwargs = mock_anthropic.messages.create.call_args.kwargs
        assert kwargs["model"] == "claude-haiku-4-5"
        assert kwargs["system"][0]["cache_control"] == {"type": "ephemeral"}
        assert kwargs["tool_choice"] == {"type": "tool", "name": "record_judgement"}
        assert kwargs["tools"][0]["name"] == "record_judgement"
        assert kwargs["tools"][0]["input_schema"]["additionalProperties"] is False

    async def test_raises_when_response_has_no_tool_use(self, mock_anthropic):
        text_block = MagicMock()
        text_block.type = "text"
        response = MagicMock()
        response.content = [text_block]
        response.stop_reason = "end_turn"
        mock_anthropic.messages.create.return_value = response

        with pytest.raises(RuntimeError, match="no tool_use"):
            await judge_answer(Clue(prompt="...", answer="X"), "Y")


class TestPersistResult:
    async def test_writes_jsonl(self, tmp_path, monkeypatch):
        monkeypatch.setattr("jeopardy.activities.DATA_DIR", tmp_path)
        history = [
            Turn(category="MATH", value=100, user_answer="4", correct=True),
            Turn(category="MATH", value=200, user_answer="x", correct=False),
        ]
        await persist_result("game-1", 100, history)

        line = (tmp_path / "results.jsonl").read_text().strip()
        payload = json.loads(line)
        assert payload["game_id"] == "game-1"
        assert payload["score"] == 100
        assert len(payload["turns"]) == 2
        assert payload["turns"][0]["correct"] is True
        assert payload["turns"][1]["user_answer"] == "x"

    async def test_appends(self, tmp_path, monkeypatch):
        monkeypatch.setattr("jeopardy.activities.DATA_DIR", tmp_path)
        await persist_result("g1", 100, [])
        await persist_result("g2", 200, [])

        lines = (tmp_path / "results.jsonl").read_text().strip().splitlines()
        assert len(lines) == 2
        assert json.loads(lines[0])["game_id"] == "g1"
        assert json.loads(lines[1])["game_id"] == "g2"

    async def test_creates_dir(self, tmp_path, monkeypatch):
        target = tmp_path / "nested" / "data"
        monkeypatch.setattr("jeopardy.activities.DATA_DIR", target)
        await persist_result("g1", 0, [])
        assert (target / "results.jsonl").exists()
