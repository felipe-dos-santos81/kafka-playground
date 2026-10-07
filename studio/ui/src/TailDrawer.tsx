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
}

// useTail keeps the last 100 records of one node container. It fetches only when
// tailSeq (from the SSE tick) moves past what it has, one fetch loop at a time
// that keeps going until it has caught up with the newest tailSeq; a failed fetch
// is retried a second later. It starts over when boot changes: the container
// restarted and numbers its records from 1 again (a tick without stats carries
// boot "", which is no restart).
function useTail(flowId: string, nodeId: string, instance: number, tailSeq: number, boot: string) {
  const [entries, setEntries] = useState<TailEntry[]>([])
  const [error, setError] = useState('')
  const [retry, setRetry] = useState(0) // bumped a second after a failed fetch, to fetch again
  const since = useRef(0)
  const latest = useRef(tailSeq) // the newest tailSeq a tick brought, read by a fetch in flight
  const fetching = useRef(false)
  const generation = useRef(0) // bumped on a restart: a fetch from before it is dropped
  const lastBoot = useRef('')
  const behind = () => since.current < latest.current

  useEffect(() => {
    if (!boot || boot === lastBoot.current) return
    lastBoot.current = boot
    generation.current++
    since.current = 0
    setEntries([])
  }, [boot])

  useEffect(() => {
    latest.current = tailSeq
  }, [tailSeq])

  useEffect(() => {
    if (fetching.current || !behind()) return
    fetching.current = true
    const gen = generation.current
    const catchUp = async () => {
      try {
        while (gen === generation.current && behind()) {
          const got = await api.tail(flowId, nodeId, since.current, instance)
          if (gen !== generation.current || got.length === 0) return
          setError('')
          since.current = got[got.length - 1].seq
          setEntries((es) => [...es, ...got].slice(-100))
        }
      } catch (e) {
        if (gen !== generation.current) return
        setError(describe(e))
        setTimeout(() => setRetry((n) => n + 1), 1000)
      } finally {
        fetching.current = false
      }
    }
    catchUp()
  }, [flowId, nodeId, instance, tailSeq, retry])

  return { entries, error }
}

// The selected node's last records (useTail), following the newest only while
// scrolled to the bottom. A producer's drawer also has Send, which renders the
// node's own key and value templates. A consumer with instances tails one of
// them, picked in the header.
export default function TailDrawer({ flowId, node, instance, instances, onInstance, tailSeq = 0, boot = '' }: Props) {
  const { entries, error } = useTail(flowId, node.id, instance, tailSeq, boot)
  const [sent, setSent] = useState('')
  const atBottom = useRef(true)
  const box = useRef<HTMLElement>(null)

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
