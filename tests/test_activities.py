import json

import pytest

from jeopardy.activities import _normalize, judge_answer, persist_result
from jeopardy.models import Clue, Turn


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


class TestJudgeAnswer:
    async def test_correct(self):
        result = await judge_answer(Clue(prompt="...", answer="Paris"), "What is Paris?")
        assert result.correct
        assert result.canonical_answer == "Paris"

    async def test_incorrect(self):
        result = await judge_answer(Clue(prompt="...", answer="Paris"), "What is London?")
        assert not result.correct
        assert result.canonical_answer == "Paris"

    async def test_canonical_answer_preserves_case(self):
        result = await judge_answer(Clue(prompt="...", answer="Eiffel Tower"), "eiffel tower")
        assert result.correct
        assert result.canonical_answer == "Eiffel Tower"


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
