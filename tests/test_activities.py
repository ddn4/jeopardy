import json
from unittest.mock import AsyncMock, MagicMock

import pytest

from jeopardy import activities
from jeopardy.activities import (
    _normalize,
    judge_answer,
    persist_result,
    select_random_game,
    select_temporal_game,
)
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


_TSV_HEADER = "round\tclue_value\tdaily_double_value\tcategory\tcomments\tanswer\tquestion\tair_date\tnotes\n"


def _tsv_row(round_: int, value: int, category: str, prompt: str, answer: str, date: str) -> str:
    return f"{round_}\t{value}\t0\t{category}\t\t{prompt}\t{answer}\t{date}\t\n"


def _full_round(round_: int, date: str, value_set: list[int]) -> str:
    """Build a complete 6-cat × 5-clue round."""
    rows = []
    for ci in range(6):
        cat = f"CAT{ci}-{date}-{round_}"
        for v in value_set:
            rows.append(_tsv_row(round_, v, cat, f"prompt {cat} {v}", f"answer {cat} {v}", date))
    return "".join(rows)


@pytest.fixture
def reset_games_index(monkeypatch):
    monkeypatch.setattr(activities, "_games_index", None)


class TestSelectRandomGame:
    async def test_returns_complete_6x5_board(self, tmp_path, monkeypatch, reset_games_index):
        tsv = _TSV_HEADER + _full_round(1, "1990-01-01", [100, 200, 300, 400, 500])
        (tmp_path / "all_questions.tsv").write_text(tsv)
        monkeypatch.setattr(activities, "DATA_DIR", tmp_path)

        board = await select_random_game()
        assert len(board.categories) == 6
        for cells in board.categories.values():
            assert [c.value for c in cells] == [100, 200, 300, 400, 500]

    async def test_skips_round_3(self, tmp_path, monkeypatch, reset_games_index):
        tsv = (
            _TSV_HEADER
            + _full_round(1, "1990-01-01", [100, 200, 300, 400, 500])
            + _tsv_row(3, 0, "FINAL", "final prompt", "final answer", "1990-01-01")
        )
        (tmp_path / "all_questions.tsv").write_text(tsv)
        monkeypatch.setattr(activities, "DATA_DIR", tmp_path)

        board = await select_random_game()
        assert "FINAL" not in board.categories

    async def test_skips_incomplete_round(self, tmp_path, monkeypatch, reset_games_index):
        # incomplete (5 cats only) + complete — should pick the complete one
        incomplete = "".join(
            _tsv_row(1, v, f"BAD{ci}", "p", "a", "1991-01-01")
            for ci in range(5)
            for v in [100, 200, 300, 400, 500]
        )
        tsv = _TSV_HEADER + incomplete + _full_round(1, "1992-01-01", [100, 200, 300, 400, 500])
        (tmp_path / "all_questions.tsv").write_text(tsv)
        monkeypatch.setattr(activities, "DATA_DIR", tmp_path)

        board = await select_random_game()
        assert all(not cat.startswith("BAD") for cat in board.categories)

    async def test_skips_mismatched_value_set(self, tmp_path, monkeypatch, reset_games_index):
        # 6 cats but mixed value sets per category — should be excluded
        bad = "".join(
            _tsv_row(1, v, f"MIX{ci}", "p", "a", "1993-01-01")
            for ci in range(6)
            for v in ([100, 200, 300, 400, 500] if ci < 3 else [200, 400, 600, 800, 1000])
        )
        tsv = _TSV_HEADER + bad + _full_round(1, "1994-01-01", [200, 400, 600, 800, 1000])
        (tmp_path / "all_questions.tsv").write_text(tsv)
        monkeypatch.setattr(activities, "DATA_DIR", tmp_path)

        board = await select_random_game()
        assert all(not cat.startswith("MIX") for cat in board.categories)

    async def test_skips_round_2(self, tmp_path, monkeypatch, reset_games_index):
        # round-2 board is well-formed but should not be selected
        tsv = (
            _TSV_HEADER
            + _full_round(1, "1990-01-01", [100, 200, 300, 400, 500])
            + _full_round(2, "1990-01-01", [400, 800, 1200, 1600, 2000])
        )
        (tmp_path / "all_questions.tsv").write_text(tsv)
        monkeypatch.setattr(activities, "DATA_DIR", tmp_path)

        for _ in range(20):
            board = await select_random_game()
            for cells in board.categories.values():
                assert [c.value for c in cells] == [100, 200, 300, 400, 500]

    async def test_caches_index_across_calls(self, tmp_path, monkeypatch, reset_games_index):
        tsv = _TSV_HEADER + _full_round(1, "1990-01-01", [100, 200, 300, 400, 500])
        (tmp_path / "all_questions.tsv").write_text(tsv)
        monkeypatch.setattr(activities, "DATA_DIR", tmp_path)

        call_count = 0
        real_loader = activities._load_games_index

        def counting_loader():
            nonlocal call_count
            call_count += 1
            return real_loader()

        monkeypatch.setattr(activities, "_load_games_index", counting_loader)
        await select_random_game()
        await select_random_game()
        await select_random_game()
        assert call_count == 1

    async def test_strips_export_escapes(self, tmp_path, monkeypatch, reset_games_index):
        # The source TSV has SQL-export escapes like \" and \' baked into text fields.
        rows = [
            _tsv_row(1, v, "\\'50'S TV", f'\\"prompt {v}\\"', f"answer\\'s {v}", "1990-01-01")
            for v in [100, 200, 300, 400, 500]
        ]
        # Pad to 6 categories so the round qualifies as complete.
        for ci in range(5):
            for v in [100, 200, 300, 400, 500]:
                rows.append(_tsv_row(1, v, f"FILLER{ci}", "p", "a", "1990-01-01"))
        (tmp_path / "all_questions.tsv").write_text(_TSV_HEADER + "".join(rows))
        monkeypatch.setattr(activities, "DATA_DIR", tmp_path)

        board = await select_random_game()
        assert "'50'S TV" in board.categories
        cell = board.categories["'50'S TV"][0]
        assert cell.prompt == '"prompt 100"'
        assert cell.answer == "answer's 100"

    async def test_maps_tsv_columns_to_clue_cell(self, tmp_path, monkeypatch, reset_games_index):
        # TSV "answer" -> ClueCell.prompt; TSV "question" -> ClueCell.answer
        tsv = _TSV_HEADER + _full_round(1, "1990-01-01", [100, 200, 300, 400, 500])
        (tmp_path / "all_questions.tsv").write_text(tsv)
        monkeypatch.setattr(activities, "DATA_DIR", tmp_path)

        board = await select_random_game()
        any_cell = next(iter(board.categories.values()))[0]
        assert any_cell.prompt.startswith("prompt ")
        assert any_cell.answer.startswith("answer ")


class TestSelectTemporalGame:
    async def test_loads_curated_board_from_json(self, tmp_path, monkeypatch):
        seed = {
            "WORKFLOWS": [
                {"value": v, "prompt": f"p{v}", "answer": f"a{v}"}
                for v in [100, 200, 300, 400, 500]
            ],
            "ACTIVITIES": [
                {"value": v, "prompt": f"q{v}", "answer": f"b{v}"}
                for v in [100, 200, 300, 400, 500]
            ],
        }
        (tmp_path / "temporal.json").write_text(json.dumps(seed))
        monkeypatch.setattr("jeopardy.activities.DATA_DIR", tmp_path)

        board = await select_temporal_game()
        assert set(board.categories.keys()) == {"WORKFLOWS", "ACTIVITIES"}
        wf = board.categories["WORKFLOWS"]
        assert [c.value for c in wf] == [100, 200, 300, 400, 500]
        assert wf[0].prompt == "p100"
        assert wf[0].answer == "a100"

    async def test_real_temporal_json_is_well_formed(self):
        # Sanity-check the actual data/temporal.json shipped with the repo.
        board = await select_temporal_game()
        assert len(board.categories) == 6
        for cat, cells in board.categories.items():
            assert [c.value for c in cells] == [100, 200, 300, 400, 500], cat


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
