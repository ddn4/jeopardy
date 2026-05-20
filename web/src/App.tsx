import { useEffect, useState } from 'react'
import { api, type AnswerResult, type PublicGameState } from './api/client'
import { Board } from './components/Board'
import { ClueModal } from './components/ClueModal'
import { ScoreBar } from './components/ScoreBar'

export function App() {
  const [gameId, setGameId] = useState<string | null>(
    () => window.location.hash.replace('#', '') || null,
  )
  const [state, setState] = useState<PublicGameState | null>(null)
  const [flash, setFlash] = useState<AnswerResult | null>(null)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    if (!gameId || state) return
    api.getGame(gameId).then(setState).catch((e) => setError(String(e)))
  }, [gameId, state])

  useEffect(() => {
    if (!flash) return
    const t = setTimeout(() => setFlash(null), 4000)
    return () => clearTimeout(t)
  }, [flash])

  useEffect(() => {
    if (!error) return
    const t = setTimeout(() => setError(null), 3000)
    return () => clearTimeout(t)
  }, [error])

  const guard = async (fn: () => Promise<void>) => {
    try {
      await fn()
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e))
    }
  }

  const startGame = async () => {
    setError(null)
    const s = await api.createGame()
    window.location.hash = s.game_id
    setGameId(s.game_id)
    setState(s)
  }

  const resetToStart = () => {
    window.location.hash = ''
    setGameId(null)
    setState(null)
  }

  if (!gameId) {
    return (
      <div className="container start">
        <h1>JEOPARDY!</h1>
        <button className="primary" onClick={startGame}>
          New Game
        </button>
        {error && <p className="error">{error}</p>}
      </div>
    )
  }

  if (!state) {
    return (
      <div className="container">
        <p>Loading…</p>
      </div>
    )
  }

  return (
    <div className="container">
      <header>
        <h1>JEOPARDY!</h1>
        <ScoreBar score={state.score} />
      </header>
      <Board
        board={state.board}
        onSelect={(category, value) =>
          guard(async () => setState(await api.selectClue(gameId, category, value)))
        }
        disabled={state.current_clue !== null || state.finished}
      />
      {state.current_clue && (
        <ClueModal
          clue={state.current_clue}
          onAnswer={(answer) =>
            guard(async () => {
              const result = await api.submitAnswer(gameId, answer)
              setState(result.state)
              setFlash(result)
            })
          }
        />
      )}
      {flash && (
        <div className={`flash ${flash.judgement}`}>
          <div className="flash-headline">
            {flash.judgement === 'correct' ? 'Correct!' : 'Incorrect'}
          </div>
          {flash.reason && (
            <>
              <div className="flash-canonical">
                Answer: <span>{flash.canonical_answer}</span>
              </div>
              <div className="flash-reason">{flash.reason}</div>
            </>
          )}
        </div>
      )}
      {state.finished ? (
        <div className="finished">
          <h2>Game over — final score: ${state.score}</h2>
          <button className="primary" onClick={resetToStart}>
            New game
          </button>
        </div>
      ) : (
        <button
          className="end"
          onClick={() => guard(async () => setState(await api.endGame(gameId)))}
        >
          End game
        </button>
      )}
      {error && <div className="error-banner">{error}</div>}
    </div>
  )
}
