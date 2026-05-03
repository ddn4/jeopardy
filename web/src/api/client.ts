export type PublicClueCell = { value: number; revealed: boolean }
export type PublicBoard = { categories: Record<string, PublicClueCell[]> }
export type CurrentClue = { category: string; value: number; prompt: string }
export type PublicGameState = {
  game_id: string
  board: PublicBoard
  score: number
  current_clue: CurrentClue | null
  finished: boolean
}
export type AnswerResult = {
  state: PublicGameState
  judgement: 'correct' | 'incorrect'
  canonical_answer: string
  reason: string | null
}

const BASE = '/api'

async function req<T>(path: string, init?: RequestInit): Promise<T> {
  const res = await fetch(`${BASE}${path}`, {
    headers: { 'Content-Type': 'application/json' },
    ...init,
  })
  if (!res.ok) {
    let detail = res.statusText
    try {
      const body = await res.json()
      if (body?.detail) detail = body.detail
    } catch {
      // body wasn't JSON; fall back to statusText
    }
    throw new Error(detail)
  }
  return res.json()
}

export type GameMode = 'random' | 'temporal'

export const api = {
  createGame: (mode: GameMode = 'random') =>
    req<PublicGameState>('/games', {
      method: 'POST',
      body: JSON.stringify({ mode }),
    }),
  getGame: (id: string) => req<PublicGameState>(`/games/${id}`),
  selectClue: (id: string, category: string, value: number) =>
    req<PublicGameState>(`/games/${id}/select`, {
      method: 'POST',
      body: JSON.stringify({ category, value }),
    }),
  submitAnswer: (id: string, answer: string) =>
    req<AnswerResult>(`/games/${id}/answer`, {
      method: 'POST',
      body: JSON.stringify({ answer }),
    }),
  endGame: (id: string) =>
    req<PublicGameState>(`/games/${id}/end`, { method: 'POST' }),
}
