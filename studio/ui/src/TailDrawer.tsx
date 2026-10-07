import { useEffect, useState } from 'react'
import { api, describe, type TailEntry } from './flow/api'
import type { StudioNode } from './nodes/types'

type Props = { flowId: string; node: StudioNode }

// The selected node's last records, fetched once a second (M3 fetches only when
// the node's tailSeq moves). A producer's drawer also has Send, which renders the
// node's own key and value templates.
export default function TailDrawer({ flowId, node }: Props) {
  const [entries, setEntries] = useState<TailEntry[]>([])
  const [error, setError] = useState('')
  const [sent, setSent] = useState('')

  useEffect(() => {
    let live = true
    let since = 0
    let busy = false // a slow answer must not be fetched twice
    const poll = () => {
      if (busy) return
      busy = true
      api
        .tail(flowId, node.id, since)
        .then(
          (got) => {
            if (!live) return
            setError('')
            if (got.length === 0) return
            const restarted = got[0].seq <= since // the node restarted and counts from 1 again
            since = got[got.length - 1].seq
            setEntries((es) => (restarted ? got : [...es, ...got]).slice(-100))
          },
          (e) => live && setError(describe(e)),
        )
        .finally(() => {
          busy = false
        })
    }
    poll()
    const timer = setInterval(poll, 1000)
    return () => {
      live = false
      clearInterval(timer)
    }
  }, [flowId, node.id])

  const send = () =>
    api.send(flowId, node.id).then(
      (r) => setSent(`sent to partition ${r.partition} at offset ${r.offset}`),
      (e) => setSent(describe(e)),
    )

  return (
    <section className="drawer">
      <header>
        <strong>{node.id}</strong> tail
        {node.type === 'producer' && <button onClick={send}>Send</button>}
        <span className="hint">{error || sent}</span>
      </header>
      {entries.length === 0 ? (
        <p className="hint">No records yet.</p>
      ) : (
        <ol>
          {entries.map((e) => (
            <li key={e.seq}>
              <code>
                p{e.partition}@{e.offset}
              </code>
              {e.key !== '' && <code>key={e.key}</code>}
              <code>{e.value}</code>
            </li>
          ))}
        </ol>
      )}
    </section>
  )
}
