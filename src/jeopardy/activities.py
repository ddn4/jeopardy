import json
import re
from pathlib import Path

from anthropic import AsyncAnthropic
from temporalio import activity

from .models import Clue, JudgeResult, Turn

DATA_DIR = Path(__file__).resolve().parents[2] / "data"

_JUDGE_MODEL = "claude-haiku-4-5"

_JUDGE_SYSTEM = """You are judging answers in a game of Jeopardy.

Apply these rules:
- Be lenient on phrasing, articles ("the", "a"), word order, capitalization, and minor spelling.
- The Jeopardy convention of phrasing answers as a question ("What is X?") is optional — accept either.
- Be strict on factual content. A different person, place, number, or date is wrong.
- For people, last name alone is acceptable when unambiguous (e.g. "Twain" for "Mark Twain").
- For dates, the year alone is sufficient unless the canonical answer specifies more.

Use the record_judgement tool to record your decision and a one-sentence reason."""

_JUDGE_TOOL = {
    "name": "record_judgement",
    "description": "Record whether the contestant's answer is correct.",
    "input_schema": {
        "type": "object",
        "properties": {
            "correct": {
                "type": "boolean",
                "description": "True if the contestant's answer is correct.",
            },
            "reason": {
                "type": "string",
                "description": "One short sentence explaining the call.",
            },
        },
        "required": ["correct", "reason"],
        "additionalProperties": False,
    },
}

_anthropic_client: AsyncAnthropic | None = None


def _get_anthropic_client() -> AsyncAnthropic:
    global _anthropic_client
    if _anthropic_client is None:
        _anthropic_client = AsyncAnthropic()
    return _anthropic_client


def _normalize(s: str) -> str:
    s = s.lower().strip()
    s = re.sub(r"^(what|who|where|when|why|how)\s+(is|are|was|were)\s+", "", s)
    s = re.sub(r"^(a|an|the)\s+", "", s)
    s = re.sub(r"[^a-z0-9 ]", "", s)
    s = re.sub(r"\s+", " ", s)
    return s.strip()


@activity.defn
async def judge_answer(clue: Clue, user_answer: str) -> JudgeResult:
    if _normalize(user_answer) == _normalize(clue.answer):
        return JudgeResult(correct=True, canonical_answer=clue.answer, reason=None)

    client = _get_anthropic_client()
    response = await client.messages.create(
        model=_JUDGE_MODEL,
        max_tokens=1024,
        system=[
            {
                "type": "text",
                "text": _JUDGE_SYSTEM,
                "cache_control": {"type": "ephemeral"},
            }
        ],
        tools=[_JUDGE_TOOL],
        tool_choice={"type": "tool", "name": "record_judgement"},
        messages=[
            {
                "role": "user",
                "content": (
                    f"Clue: {clue.prompt}\n"
                    f"Canonical answer: {clue.answer}\n"
                    f"Contestant said: {user_answer}\n\n"
                    "Was the contestant correct?"
                ),
            }
        ],
    )

    for block in response.content:
        if block.type == "tool_use" and block.name == "record_judgement":
            data = block.input
            return JudgeResult(
                correct=bool(data["correct"]),
                canonical_answer=clue.answer,
                reason=data.get("reason"),
            )
    raise RuntimeError(
        f"judge_answer: no tool_use block in response (stop_reason={response.stop_reason})"
    )


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
