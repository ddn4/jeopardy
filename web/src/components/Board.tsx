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
  return (
    <div className="board">
      {categories.map((cat) => (
        <div className="column" key={cat}>
          <div className="category">{cat}</div>
          {board.categories[cat].map((cell) => (
            <button
              key={cell.value}
              className={`cell ${cell.revealed ? 'revealed' : ''}`}
              disabled={cell.revealed || disabled}
              onClick={() => onSelect(cat, cell.value)}
            >
              {cell.revealed ? '' : `$${cell.value}`}
            </button>
          ))}
        </div>
      ))}
    </div>
  )
}
