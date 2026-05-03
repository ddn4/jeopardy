import type { PublicBoard } from '../api/client'

export function Board({
  board,
  onSelect,
  disabled,
}: {
  board: PublicBoard
  onSelect: (category: string, value: number) => void
  disabled: boolean
}) {
  const categories = Object.keys(board.categories)
  if (categories.length === 0) return <p>Loading board…</p>
  const rowCount = Math.max(...categories.map((c) => board.categories[c].length))
  return (
    <div
      className="board"
      style={{ gridTemplateColumns: `repeat(${categories.length}, 1fr)` }}
    >
      {categories.map((cat) => (
        <div className="category" key={`h-${cat}`}>
          {cat}
        </div>
      ))}
      {Array.from({ length: rowCount }).flatMap((_, rowIdx) =>
        categories.map((cat) => {
          const cell = board.categories[cat][rowIdx]
          const key = `${cat}-${rowIdx}`
          if (!cell) return <div className="cell empty" key={key} />
          return (
            <button
              key={key}
              className={`cell ${cell.revealed ? 'revealed' : ''}`}
              disabled={cell.revealed || disabled}
              onClick={() => onSelect(cat, cell.value)}
            >
              {cell.revealed ? '' : `$${cell.value}`}
            </button>
          )
        }),
      )}
    </div>
  )
}
