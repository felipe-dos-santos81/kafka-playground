import { useEffect, useRef, useState, type UIEvent } from 'react'
import { api, describe, type TailEntry } from './flow/api'
import type { StudioNode } from './nodes/types'

type Props = {
  flowId: string
  node: StudioNode
  instance: number // 0: the node's only container; else one of instances
  instances: number[] // a consumer's instance numbers; empty when it runs one container
  onInstance: (i: number) => void
  tailSeq?: number
  boot?: string
  tick: number // counts the flow's ticks
}

// The selected node's last records. It fetches only when the node's tailSeq (from
// the SSE tick) moves past what it has, one fetch at a time (when one ends, it
// fetches again if tailSeq moved meanwhile; a failed one is retried on the next
// tick), and starts over when boot changes: the container restarted and numbers
// its records from 1 again. It follows the newest record only while scrolled to
// the bottom. A producer's drawer also has Send, which renders the node's own key
// and value templates. A consumer with instances tails one of them, picked in the
// header.
export default function TailDrawer({ flowId, node, instance, instances, onInstance, tailSeq = 0, boot = '', tick }: Props) {
  const [entries, setEntries] = useState<TailEntry[]>([])
  const [error, setError] = useState('')
  const [sent, setSent] = useState('')
  const [fetched, setFetched] = useState(0) // counts fetches that brought records, to chase tailSeq
  const since = useRef(0)
  const fetching = useRef(false)
  const generation = useRef(0) // bumped on a restart: a fetch from before it is dropped
  const atBottom = useRef(true)
  const box = useRef<HTMLElement>(null)

  // A tick without stats carries boot "": only a different non-empty boot is a restart.
  const lastBoot = useRef('')
  useEffect(() => {
    if (!boot || boot === lastBoot.current) return
    lastBoot.current = boot
    generation.current++
    since.current = 0
    setEntries([])
  }, [boot])

  useEffect(() => {
    if (fetching.current || tailSeq <= since.current) return
    fetching.current = true
    const gen = generation.current
    api.tail(flowId, node.id, since.current, instance).then(
      (got) => {
        fetching.current = false
        setError('')
        if (gen !== generation.current || got.length === 0) return
        since.current = got[got.length - 1].seq
        setEntries((es) => [...es, ...got].slice(-100))
        setFetched((n) => n + 1)
      },
      (e) => {
        fetching.current = false
        if (gen !== generation.current) return
        setError(describe(e))
      },
    )
  }, [flowId, node.id, instance, boot, tailSeq, tick, fetched])

  // Follow the newest record, unless scrolled up to read an older one.
  useEffect(() => {
    if (atBottom.current) box.current?.scrollTo({ top: box.current.scrollHeight })
  }, [entries])
  const onScroll = (e: UIEvent<HTMLElement>) => {
    const el = e.currentTarget
    atBottom.current = el.scrollHeight - el.scrollTop - el.clientHeight < 8
  }

  const send = () =>
    api.send(flowId, node.id).then(
      (r) => setSent(`sent to partition ${r.partition} at offset ${r.offset}`),
      (e) => setSent(describe(e)),
    )

  return (
    <section className="drawer" ref={box} onScroll={onScroll}>
      <header>
        <strong>{node.id}</strong> tail
        {instances.length > 0 && (
          <select value={instance} onChange={(e) => onInstance(Number(e.target.value))}>
            {instances.map((i) => (
              <option key={i} value={i}>
                instance {i}
              </option>
            ))}
          </select>
        )}
        {node.type === 'producer' && <button onClick={send}>Send</button>}
        {sent && <span className="hint">{sent}</span>}
        {error && <span className="error">{error}</span>}
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
