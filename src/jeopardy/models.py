from typing import Literal

from pydantic import BaseModel


class Clue(BaseModel):
    prompt: str
    answer: str


class ClueCell(BaseModel):
    value: int
    revealed: bool = False
    prompt: str
    answer: str


class PublicClueCell(BaseModel):
    value: int
    revealed: bool


class Board(BaseModel):
    categories: dict[str, list[ClueCell]]


class PublicBoard(BaseModel):
    categories: dict[str, list[PublicClueCell]]


class Turn(BaseModel):
    category: str
    value: int
    user_answer: str
    correct: bool


class CurrentClue(BaseModel):
    category: str
    value: int
    prompt: str


class PublicGameState(BaseModel):
    game_id: str
    board: PublicBoard
    score: int
    current_clue: CurrentClue | None
    finished: bool


class SelectClueInput(BaseModel):
    category: str
    value: int


class SubmitAnswerInput(BaseModel):
    answer: str


class JudgeResult(BaseModel):
    correct: bool
    canonical_answer: str
    reason: str | None = None


class AnswerResult(BaseModel):
    state: PublicGameState
    judgement: Literal["correct", "incorrect"]
    canonical_answer: str
    reason: str | None = None
