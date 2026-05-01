export function ScoreBar({ score }: { score: number }) {
  return <div className={`score ${score < 0 ? 'negative' : ''}`}>${score}</div>
}
