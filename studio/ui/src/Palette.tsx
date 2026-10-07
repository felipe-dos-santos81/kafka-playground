import type { NodeType } from './flow/schema'

export const DRAG_TYPE = 'application/x-studio-node'

const ITEMS: NodeType[] = ['producer', 'topic', 'consumer'] // transform joins in M5

export default function Palette() {
  return (
    <section>
      <h2>Nodes</h2>
      {ITEMS.map((type) => (
        <div
          key={type}
          className={`palette-item ${type}`}
          draggable
          onDragStart={(e) => {
            e.dataTransfer.setData(DRAG_TYPE, type)
            e.dataTransfer.effectAllowed = 'move'
          }}
        >
          {type}
        </div>
      ))}
      <p className="hint">Drag onto the canvas, then wire Producer → Topic → Consumer. Backspace deletes.</p>
    </section>
  )
}
