# Jeopardy on Temporal

A Jeopardy-style game where each game session is a long-running Temporal
workflow. Player input arrives as workflow signals; the React UI reads game
state via a workflow query.

```
React (Vite, :5173)
   |
   v
FastAPI gateway (:8000)   -- start / signal / query -->   Temporal (:7233)
                                                                 |
                                                                 v
                                                          Python worker
                                                          (workflow + activities)
```

The FastAPI layer exists only to translate HTTP into Temporal client calls —
browsers can't speak Temporal's gRPC protocol directly.

## Prerequisites

- Python 3.11+
- Node 20+
- [`uv`](https://github.com/astral-sh/uv): `brew install uv`
- [Temporal CLI](https://docs.temporal.io/cli): `brew install temporal`

## First-time setup

```bash
# Python deps + venv
uv sync

# Frontend deps
cd web && npm install && cd ..
```

## Running the stack (4 terminals)

```bash
# 1. Temporal dev server (frontend on :7233, Web UI on :8233)
temporal server start-dev

# 2. Worker
uv run python -m jeopardy.worker

# 3. API
uv run uvicorn jeopardy.api:app --reload --port 8000

# 4. Frontend
cd web && npm run dev
```

Then open http://localhost:5173.

## Project layout

```
jeopardy/
├── pyproject.toml
├── data/
│   └── all_questions.tsv     # archive of real Jeopardy clues (rounds 1, 2, 3)
├── src/jeopardy/
│   ├── models.py             # pydantic types (state, updates, results)
│   ├── workflows.py          # JeopardyGameWorkflow (updates + query)
│   ├── activities.py         # select_random_game, judge_answer, persist_result
│   ├── api.py                # FastAPI gateway (calls workflow updates)
│   └── worker.py             # worker entrypoint
└── web/
    └── src/
        ├── App.tsx
        ├── api/client.ts
        └── components/{Board,ClueModal,ScoreBar}.tsx
```

## How a game flows

Player actions are **workflow updates** — request-reply RPCs that mutate
state and return the new state synchronously. The frontend never polls; it
just stores whatever each update returned.

1. `POST /games` → API starts a `JeopardyGameWorkflow`. The workflow's first
   step calls the `select_random_game` activity, which picks a random complete
   6×5 round-1 board from `data/all_questions.tsv`.
   The API blocks on the `wait_until_ready` update and returns the initial
   `PublicGameState`. Workflow id is stored in the URL hash so refresh resumes.
2. Click a cell → `POST /games/{id}/select` → `select_clue` update returns
   the state with `current_clue` set.
3. Submit answer → `POST /games/{id}/answer` → `submit_answer` update runs
   the `judge_answer` activity and returns
   `{state, judgement, canonical_answer}`. The frontend updates state and
   flashes the result.
4. Game ends when the board is complete (auto) or `end_game` update arrives.
   Either path runs `persist_result` and finalizes via `_finalize()`.
5. `GET /games/{id}` is a workflow query, used only on page load / refresh.

You can watch the full event history at http://localhost:8233 — every
signal, query, activity, and timer is recorded.

## Customizing

- **Different clues**: every new game samples a complete 6×5 board from
  `data/all_questions.tsv`. The first call to `select_random_game` parses the
  TSV (~1s) and caches the index of valid boards in worker memory.
- **Tweak judging**: `judge_answer` in `activities.py` does a fast normalized
  string match, then falls back to an Anthropic LLM (`claude-haiku-4-5`) for
  fuzzy correctness with a one-sentence reason.
