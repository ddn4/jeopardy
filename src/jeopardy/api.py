import json
import uuid
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse
from temporalio.client import Client, WorkflowUpdateFailedError
from temporalio.contrib.pydantic import pydantic_data_converter
from temporalio.exceptions import ApplicationError

from . import TASK_QUEUE
from .activities import DATA_DIR
from .models import (
    AnswerResult,
    Board,
    ClueCell,
    PublicGameState,
    SelectClueInput,
    SubmitAnswerInput,
)
from .workflows import JeopardyGameWorkflow

_client: Client | None = None


async def _get_client() -> Client:
    global _client
    if _client is None:
        _client = await Client.connect(
            "localhost:7233",
            data_converter=pydantic_data_converter,
        )
    return _client


def _load_board() -> Board:
    raw = json.loads((DATA_DIR / "questions.json").read_text())
    return Board(
        categories={
            cat: [ClueCell(value=c["value"], prompt=c["prompt"], answer=c["answer"]) for c in cells]
            for cat, cells in raw.items()
        }
    )


@asynccontextmanager
async def lifespan(_: FastAPI):
    await _get_client()
    yield


app = FastAPI(title="Jeopardy", lifespan=lifespan)


@app.exception_handler(WorkflowUpdateFailedError)
async def update_rejected(_: Request, exc: WorkflowUpdateFailedError) -> JSONResponse:
    cause = exc.cause
    detail = cause.message if isinstance(cause, ApplicationError) else str(exc)
    return JSONResponse(status_code=400, content={"detail": detail})


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}


@app.post("/games")
async def create_game() -> PublicGameState:
    client = await _get_client()
    game_id = f"game-{uuid.uuid4().hex[:8]}"
    handle = await client.start_workflow(
        JeopardyGameWorkflow.run,
        _load_board(),
        id=game_id,
        task_queue=TASK_QUEUE,
    )
    return await handle.query(JeopardyGameWorkflow.get_state)


@app.get("/games/{game_id}")
async def get_game(game_id: str) -> PublicGameState:
    client = await _get_client()
    handle = client.get_workflow_handle(game_id)
    try:
        return await handle.query(JeopardyGameWorkflow.get_state)
    except Exception as e:
        raise HTTPException(status_code=404, detail=str(e))


@app.post("/games/{game_id}/select")
async def select_clue(game_id: str, payload: SelectClueInput) -> PublicGameState:
    client = await _get_client()
    handle = client.get_workflow_handle_for(JeopardyGameWorkflow.run, game_id)
    return await handle.execute_update(JeopardyGameWorkflow.select_clue, payload)


@app.post("/games/{game_id}/answer")
async def submit_answer(game_id: str, payload: SubmitAnswerInput) -> AnswerResult:
    client = await _get_client()
    handle = client.get_workflow_handle_for(JeopardyGameWorkflow.run, game_id)
    return await handle.execute_update(JeopardyGameWorkflow.submit_answer, payload)


@app.post("/games/{game_id}/end")
async def end_game(game_id: str) -> PublicGameState:
    client = await _get_client()
    handle = client.get_workflow_handle_for(JeopardyGameWorkflow.run, game_id)
    return await handle.execute_update(JeopardyGameWorkflow.end_game)
