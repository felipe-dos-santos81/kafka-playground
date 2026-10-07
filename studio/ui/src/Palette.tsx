import type { NodeType } from './flow/schema'

export const DRAG_TYPE = 'application/x-studio-node'

const ITEMS: { type: NodeType; label: string }[] = [
  { type: 'producer', label: 'Producer' },
  { type: 'topic', label: 'Topic' },
  { type: 'consumer', label: 'Consumer' },
]

export default function Palette() {
  return (
    <section>
      <h2>Nodes</h2>
      {ITEMS.map((it) => (
        <div
          key={it.type}
          className={`palette-item ${it.type}`}
          draggable
          onDragStart={(e) => {
            e.dataTransfer.setData(DRAG_TYPE, it.type)
            e.dataTransfer.effectAllowed = 'move'
          }}
        >
          {it.label}
        </div>
      ))}
      <p className="hint">Drag onto the canvas, then wire Producer → Topic → Consumer. Backspace deletes.</p>
    </section>
  )
}
