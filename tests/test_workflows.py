import uuid
from unittest.mock import AsyncMock, MagicMock

import pytest
from temporalio import activity
from temporalio.client import WorkflowUpdateFailedError
from temporalio.contrib.pydantic import pydantic_data_converter
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker

from jeopardy import TASK_QUEUE
from jeopardy.activities import judge_answer, persist_result
from jeopardy.models import (
    Board,
    ClueCell,
    SelectClueInput,
    StartGameInput,
    SubmitAnswerInput,
)
from jeopardy.workflows import JeopardyGameWorkflow


@pytest.fixture(autouse=True)
def mock_anthropic(monkeypatch):
    """Default the LLM judge to 'incorrect' for any non-exact match — keeps tests offline."""
    block = MagicMock()
    block.type = "tool_use"
    block.name = "record_judgement"
    block.input = {"correct": False, "reason": "mock: incorrect"}
    response = MagicMock()
    response.content = [block]
    response.stop_reason = "tool_use"
    client = MagicMock()
    client.messages.create = AsyncMock(return_value=response)
    monkeypatch.setattr("jeopardy.activities._anthropic_client", client)
    return client


def _board(cells: dict[str, list[tuple[int, str, str]]]) -> Board:
    return Board(
        categories={
            cat: [ClueCell(value=v, prompt=p, answer=a) for v, p, a in items]
            for cat, items in cells.items()
        }
    )


class _BoardInjector:
    """Holds the next Board to return from select_random_game / select_temporal_game."""

    def __init__(self) -> None:
        self.random: Board | None = None
        self.temporal: Board | None = None

    def set(self, board: Board) -> None:
        self.random = board

    def set_temporal(self, board: Board) -> None:
        self.temporal = board


@pytest.fixture
async def worker(monkeypatch, tmp_path):
    monkeypatch.setattr("jeopardy.activities.DATA_DIR", tmp_path)
    injector = _BoardInjector()

    @activity.defn(name="select_random_game")
    async def fake_select_random_game() -> Board:
        assert injector.random is not None, "test must call injector.set(board) before _start"
        return injector.random

    @activity.defn(name="select_temporal_game")
    async def fake_select_temporal_game() -> Board:
        assert injector.temporal is not None, "test must call injector.set_temporal(board) first"
        return injector.temporal

    async with await WorkflowEnvironment.start_local(
        data_converter=pydantic_data_converter,
    ) as env:
        async with Worker(
            env.client,
            task_queue=TASK_QUEUE,
            workflows=[JeopardyGameWorkflow],
            activities=[
                judge_answer,
                persist_result,
                fake_select_random_game,
                fake_select_temporal_game,
            ],
        ):
            yield env, injector


async def _start(worker_ctx, board: Board, mode: str = "random"):
    env, injector = worker_ctx
    if mode == "temporal":
        injector.set_temporal(board)
    else:
        injector.set(board)
    handle = await env.client.start_workflow(
        JeopardyGameWorkflow.run,
        StartGameInput(mode=mode),
        id=f"test-{uuid.uuid4().hex[:8]}",
        task_queue=TASK_QUEUE,
    )
    await handle.execute_update(JeopardyGameWorkflow.wait_until_ready)
    return handle


async def test_correct_answer_increments_score(worker):
    board = _board({"X": [(100, "p1", "a1"), (200, "p2", "a2")]})
    handle = await _start(worker, board)

    state = await handle.execute_update(
        JeopardyGameWorkflow.select_clue, SelectClueInput(category="X", value=100)
    )
    assert state.current_clue is not None
    assert state.current_clue.prompt == "p1"
    assert state.current_clue.value == 100

    result = await handle.execute_update(
        JeopardyGameWorkflow.submit_answer, SubmitAnswerInput(answer="a1")
    )
    assert result.judgement == "correct"
    assert result.canonical_answer == "a1"
    assert result.state.score == 100
    assert result.state.current_clue is None
    assert result.state.board.categories["X"][0].revealed
    assert not result.state.finished


async def test_incorrect_answer_decrements_score(worker):
    board = _board({"X": [(100, "p1", "right"), (200, "p2", "right2")]})
    handle = await _start(worker, board)

    await handle.execute_update(
        JeopardyGameWorkflow.select_clue, SelectClueInput(category="X", value=100)
    )
    result = await handle.execute_update(
        JeopardyGameWorkflow.submit_answer, SubmitAnswerInput(answer="wrong")
    )
    assert result.judgement == "incorrect"
    assert result.canonical_answer == "right"
    assert result.state.score == -100


async def test_completing_board_finalizes_workflow(worker):
    board = _board({"X": [(100, "p", "a")]})
    handle = await _start(worker, board)

    await handle.execute_update(
        JeopardyGameWorkflow.select_clue, SelectClueInput(category="X", value=100)
    )
    result = await handle.execute_update(
        JeopardyGameWorkflow.submit_answer, SubmitAnswerInput(answer="a")
    )
    assert result.state.finished
    assert await handle.result() == 100


async def test_end_game_finalizes_early(worker):
    board = _board({"X": [(100, "p1", "a1"), (200, "p2", "a2")]})
    handle = await _start(worker, board)

    state = await handle.execute_update(JeopardyGameWorkflow.end_game)
    assert state.finished
    assert state.score == 0
    assert await handle.result() == 0


async def test_get_state_query_initial(worker):
    board = _board({"X": [(100, "p", "a")], "Y": [(100, "p", "a")]})
    handle = await _start(worker, board)

    state = await handle.query(JeopardyGameWorkflow.get_state)
    assert state.score == 0
    assert state.current_clue is None
    assert not state.finished
    assert set(state.board.categories.keys()) == {"X", "Y"}
    assert all(not c.revealed for c in state.board.categories["X"])


async def test_select_unknown_category_rejected(worker):
    board = _board({"X": [(100, "p", "a")]})
    handle = await _start(worker, board)

    with pytest.raises(WorkflowUpdateFailedError):
        await handle.execute_update(
            JeopardyGameWorkflow.select_clue, SelectClueInput(category="Y", value=100)
        )


async def test_select_unknown_value_rejected(worker):
    board = _board({"X": [(100, "p", "a")]})
    handle = await _start(worker, board)

    with pytest.raises(WorkflowUpdateFailedError):
        await handle.execute_update(
            JeopardyGameWorkflow.select_clue, SelectClueInput(category="X", value=999)
        )


async def test_select_when_clue_active_rejected(worker):
    board = _board({"X": [(100, "p1", "a1"), (200, "p2", "a2")]})
    handle = await _start(worker, board)

    await handle.execute_update(
        JeopardyGameWorkflow.select_clue, SelectClueInput(category="X", value=100)
    )
    with pytest.raises(WorkflowUpdateFailedError):
        await handle.execute_update(
            JeopardyGameWorkflow.select_clue, SelectClueInput(category="X", value=200)
        )


async def test_select_revealed_cell_rejected(worker):
    board = _board({"X": [(100, "p1", "a1"), (200, "p2", "a2")]})
    handle = await _start(worker, board)

    await handle.execute_update(
        JeopardyGameWorkflow.select_clue, SelectClueInput(category="X", value=100)
    )
    await handle.execute_update(
        JeopardyGameWorkflow.submit_answer, SubmitAnswerInput(answer="a1")
    )
    with pytest.raises(WorkflowUpdateFailedError):
        await handle.execute_update(
            JeopardyGameWorkflow.select_clue, SelectClueInput(category="X", value=100)
        )


async def test_submit_without_select_rejected(worker):
    board = _board({"X": [(100, "p", "a")]})
    handle = await _start(worker, board)

    with pytest.raises(WorkflowUpdateFailedError):
        await handle.execute_update(
            JeopardyGameWorkflow.submit_answer, SubmitAnswerInput(answer="a")
        )


async def test_empty_answer_rejected(worker):
    board = _board({"X": [(100, "p", "a")]})
    handle = await _start(worker, board)

    await handle.execute_update(
        JeopardyGameWorkflow.select_clue, SelectClueInput(category="X", value=100)
    )
    with pytest.raises(WorkflowUpdateFailedError):
        await handle.execute_update(
            JeopardyGameWorkflow.submit_answer, SubmitAnswerInput(answer="   ")
        )


async def test_wait_until_ready_returns_loaded_board(worker):
    board = _board({"X": [(100, "p1", "a1"), (200, "p2", "a2")], "Y": [(100, "p", "a")]})
    handle = await _start(worker, board)

    state = await handle.execute_update(JeopardyGameWorkflow.wait_until_ready)
    assert set(state.board.categories.keys()) == {"X", "Y"}
    assert state.score == 0
    assert state.current_clue is None
    assert not state.finished


async def test_temporal_mode_uses_temporal_activity(worker):
    random_board = _board({"R": [(100, "p", "a")]})
    temporal_board = _board(
        {"WORKFLOWS": [(100, "temporal prompt", "Workflow Definition")]}
    )
    env, injector = worker
    injector.set(random_board)
    injector.set_temporal(temporal_board)

    handle = await env.client.start_workflow(
        JeopardyGameWorkflow.run,
        StartGameInput(mode="temporal"),
        id=f"test-{uuid.uuid4().hex[:8]}",
        task_queue=TASK_QUEUE,
    )
    state = await handle.execute_update(JeopardyGameWorkflow.wait_until_ready)
    assert "WORKFLOWS" in state.board.categories
    assert "R" not in state.board.categories


async def test_score_accumulates_across_turns(worker):
    board = _board(
        {
            "X": [(100, "p1", "a1"), (200, "p2", "a2")],
            "Y": [(100, "p3", "a3"), (200, "p4", "a4")],
        }
    )
    handle = await _start(worker, board)

    async def play(category: str, value: int, answer: str) -> int:
        await handle.execute_update(
            JeopardyGameWorkflow.select_clue,
            SelectClueInput(category=category, value=value),
        )
        result = await handle.execute_update(
            JeopardyGameWorkflow.submit_answer, SubmitAnswerInput(answer=answer)
        )
        return result.state.score

    assert await play("X", 100, "a1") == 100
    assert await play("X", 200, "wrong") == -100
    assert await play("Y", 100, "a3") == 0
    assert await play("Y", 200, "a4") == 200
