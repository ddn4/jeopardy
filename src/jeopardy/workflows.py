from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy

with workflow.unsafe.imports_passed_through():
    from .activities import (
        judge_answer,
        persist_result,
        select_random_game,
        select_temporal_game,
    )
    from .models import (
        AnswerResult,
        Board,
        Clue,
        ClueCell,
        CurrentClue,
        GameMode,
        JudgeResult,
        PublicBoard,
        PublicClueCell,
        PublicGameState,
        SelectClueInput,
        StartGameInput,
        SubmitAnswerInput,
        Turn,
    )


@workflow.defn
class JeopardyGameWorkflow:
    @workflow.init
    def __init__(self, input: StartGameInput) -> None:
        self.mode: GameMode = input.mode
        self.board: Board | None = None
        self.score: int = 0
        self.current_clue: CurrentClue | None = None
        self.history: list[Turn] = []
        self.finished: bool = False

    @workflow.run
    async def run(self, input: StartGameInput) -> int:
        loader = select_temporal_game if self.mode == "temporal" else select_random_game
        self.board = await workflow.execute_activity(
            loader,
            start_to_close_timeout=timedelta(seconds=15),
            retry_policy=RetryPolicy(maximum_attempts=3),
        )
        await workflow.wait_condition(lambda: self.finished)
        return self.score

    @workflow.update
    async def wait_until_ready(self) -> PublicGameState:
        await workflow.wait_condition(lambda: self.board is not None)
        return self._public_state()

    @workflow.update
    async def select_clue(self, payload: SelectClueInput) -> PublicGameState:
        cell = self._cell(payload.category, payload.value)
        assert cell is not None
        self.current_clue = CurrentClue(
            category=payload.category, value=payload.value, prompt=cell.prompt
        )
        return self._public_state()

    @select_clue.validator
    def _validate_select_clue(self, payload: SelectClueInput) -> None:
        if self.board is None:
            raise ValueError("game still loading")
        if self.finished:
            raise ValueError("game already finished")
        if self.current_clue is not None:
            raise ValueError("a clue is already active")
        cell = self._cell(payload.category, payload.value)
        if cell is None:
            raise ValueError(f"no clue at {payload.category}/{payload.value}")
        if cell.revealed:
            raise ValueError("clue already revealed")

    @workflow.update
    async def submit_answer(self, payload: SubmitAnswerInput) -> AnswerResult:
        assert self.current_clue is not None
        cell = self._cell(self.current_clue.category, self.current_clue.value)
        assert cell is not None

        result: JudgeResult = await workflow.execute_activity(
            judge_answer,
            args=[Clue(prompt=cell.prompt, answer=cell.answer), payload.answer],
            start_to_close_timeout=timedelta(seconds=30),
            retry_policy=RetryPolicy(maximum_attempts=3),
        )

        cell.revealed = True
        self.score += cell.value if result.correct else -cell.value
        self.history.append(
            Turn(
                category=self.current_clue.category,
                value=self.current_clue.value,
                user_answer=payload.answer,
                correct=result.correct,
            )
        )
        self.current_clue = None

        if self._board_complete():
            await self._finalize()

        return AnswerResult(
            state=self._public_state(),
            judgement="correct" if result.correct else "incorrect",
            canonical_answer=result.canonical_answer,
            reason=result.reason,
        )

    @submit_answer.validator
    def _validate_submit_answer(self, payload: SubmitAnswerInput) -> None:
        if self.board is None:
            raise ValueError("game still loading")
        if self.finished:
            raise ValueError("game already finished")
        if self.current_clue is None:
            raise ValueError("no clue selected")
        if not payload.answer.strip():
            raise ValueError("answer is empty")

    @workflow.update
    async def end_game(self) -> PublicGameState:
        if not self.finished:
            await self._finalize()
        return self._public_state()

    @workflow.query
    def get_state(self) -> PublicGameState:
        return self._public_state()

    async def _finalize(self) -> None:
        await workflow.execute_activity(
            persist_result,
            args=[workflow.info().workflow_id, self.score, self.history],
            start_to_close_timeout=timedelta(seconds=10),
        )
        self.finished = True

    def _public_state(self) -> PublicGameState:
        categories: dict[str, list[PublicClueCell]] = (
            {}
            if self.board is None
            else {
                cat: [PublicClueCell(value=c.value, revealed=c.revealed) for c in cells]
                for cat, cells in self.board.categories.items()
            }
        )
        return PublicGameState(
            game_id=workflow.info().workflow_id,
            board=PublicBoard(categories=categories),
            score=self.score,
            current_clue=self.current_clue,
            finished=self.finished,
        )

    def _cell(self, category: str, value: int) -> ClueCell | None:
        if self.board is None:
            return None
        for c in self.board.categories.get(category, []):
            if c.value == value:
                return c
        return None

    def _board_complete(self) -> bool:
        if self.board is None:
            return False
        return all(c.revealed for cells in self.board.categories.values() for c in cells)
