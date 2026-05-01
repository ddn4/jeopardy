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
│   └── questions.json        # seed clues (6 categories × 5 values)
├── src/jeopardy/
│   ├── models.py             # pydantic types (state, updates, results)
│   ├── workflows.py          # JeopardyGameWorkflow (updates + query)
│   ├── activities.py         # judge_answer, persist_result
│   ├── api.py                # FastAPI gateway (loads board, calls updates)
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

1. `POST /games` → API reads `data/questions.json`, starts a
   `JeopardyGameWorkflow` with the `Board` as input, and returns the initial
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

- **Replace clues**: edit `data/questions.json`. Categories must each have the
  same set of values; the board renders a 6×5 grid by default.
- **Smarter judging**: `judge_answer` in `activities.py` is a normalized
  string match. Swap it for an LLM call without touching the workflow.
- **Multiplayer**: extend the workflow to track multiple player scores and
  add buzz-in coordination via additional signals + a `wait_condition` race.
