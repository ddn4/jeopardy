import json
import re
from pathlib import Path

from temporalio import activity

from .models import Clue, JudgeResult, Turn

DATA_DIR = Path(__file__).resolve().parents[2] / "data"


def _normalize(s: str) -> str:
    s = s.lower().strip()
    s = re.sub(r"^(what|who|where|when|why|how)\s+(is|are|was|were)\s+", "", s)
    s = re.sub(r"^(a|an|the)\s+", "", s)
    s = re.sub(r"[^a-z0-9 ]", "", s)
    s = re.sub(r"\s+", " ", s)
    return s.strip()


@activity.defn
async def judge_answer(clue: Clue, user_answer: str) -> JudgeResult:
    correct = _normalize(user_answer) == _normalize(clue.answer)
    return JudgeResult(correct=correct, canonical_answer=clue.answer)


@activity.defn
async def persist_result(game_id: str, score: int, history: list[Turn]) -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    payload = {
        "game_id": game_id,
        "score": score,
        "turns": [t.model_dump() for t in history],
    }
    with (DATA_DIR / "results.jsonl").open("a") as f:
        f.write(json.dumps(payload) + "\n")
