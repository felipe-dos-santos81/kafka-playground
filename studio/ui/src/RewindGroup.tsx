import { useEffect, useState } from 'react'
import { api, describe, type RewindTo } from './flow/api'

type Props = {
  flowId: string
  node: string // a consumer
  running: boolean // the flow runs: the broker refuses to move a group with members
  dirty: boolean // unsaved edits: a rewind uses the saved group and topic
}

const TARGETS: RewindTo[] = ['earliest', 'latest']

// A consumer's "Rewind group" row in the Inspector: sets its group to the start
// or the end of its topic, so the next deploy reads from there whatever
// auto.offset.reset says.
export default function RewindGroup({ flowId, node, running, dirty }: Props) {
  const [result, setResult] = useState('') // the last rewind's outcome, until the flow runs again
  useEffect(() => {
    if (running) setResult('')
  }, [running])

  const blocked = running
    ? 'Stop the flow to rewind its group.'
    : dirty
      ? 'Save first: a rewind uses the saved group and topic.'
      : ''
  const rewind = (to: RewindTo) =>
    api.rewind(flowId, node, to).then(
      (r) => setResult(`${r.group} on ${r.topic}: ${r.partitions} partition${r.partitions === 1 ? '' : 's'} rewound to ${r.to}`),
      (e) => setResult(describe(e)),
    )

  return (
    <>
      <label id="inspector-rewind">Rewind group</label>
      <div role="group" aria-labelledby="inspector-rewind" className="buttons">
        {TARGETS.map((to) => (
          <button key={to} disabled={blocked !== ''} onClick={() => rewind(to)}>
            to {to}
          </button>
        ))}
      </div>
      <p className="hint">{blocked || result || 'Sets where the next deploy starts reading, whatever auto.offset.reset says.'}</p>
    </>
  )
}
