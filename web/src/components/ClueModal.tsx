import { useEffect, useRef, useState } from 'react'
import type { CurrentClue } from '../api/client'

export function ClueModal({
  clue,
  onAnswer,
}: {
  clue: CurrentClue
  onAnswer: (answer: string) => void
}) {
  const [answer, setAnswer] = useState('')
  const [submitted, setSubmitted] = useState(false)
  const inputRef = useRef<HTMLInputElement>(null)

  useEffect(() => {
    inputRef.current?.focus()
  }, [])

  const submit = () => {
    if (!answer.trim() || submitted) return
    setSubmitted(true)
    onAnswer(answer)
  }

  return (
    <div className="modal-backdrop">
      <div className="modal">
        <div className="clue-header">
          {clue.category} — ${clue.value}
        </div>
        <div className="clue-prompt">{clue.prompt}</div>
        <input
          ref={inputRef}
          value={answer}
          onChange={(e) => setAnswer(e.target.value)}
          onKeyDown={(e) => e.key === 'Enter' && submit()}
          placeholder="What is..."
          disabled={submitted}
        />
        <button onClick={submit} disabled={submitted}>
          Submit
        </button>
      </div>
    </div>
  )
}
