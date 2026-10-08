import { useEdges, useNodes } from '@xyflow/react'
import { wiredTopics } from './flow/router'
import type { RouterData, Rule, StudioNode } from './nodes/types'

type Props = {
  id: string // the router's node id
  data: RouterData
  set: (patch: Partial<RouterData>) => void
}

// A router's rules in order (first match wins), each a condition and one of the
// router's wired topics, and its default. A rule or default naming a topic the
// router is not wired to stays, marked, until it is changed: Deploy refuses it.
export default function RouterRules({ id, data, set }: Props) {
  const topics = wiredTopics(id, useNodes<StudioNode>(), useEdges())
  const wired = new Set(topics.map((t) => t.id))
  const setRules = (rules: Rule[]) => set({ rules })
  const update = (i: number, patch: Partial<Rule>) => setRules(data.rules.map((r, j) => (j === i ? { ...r, ...patch } : r)))
  const move = (i: number, by: number) => {
    const rules = [...data.rules]
    ;[rules[i], rules[i + by]] = [rules[i + by], rules[i]]
    setRules(rules)
  }
  // A select's options: the wired topics, plus the current value when it is not one of them.
  const options = (current: string) => (
    <>
      {current !== '' && !wired.has(current) && <option value={current}>{current} (not wired)</option>}
      {topics.map((t) => (
        <option key={t.id} value={t.id}>
          {t.name}
        </option>
      ))}
    </>
  )

  return (
    <>
      {data.rules.map((r, i) => (
        <fieldset key={i} className="rule">
          <legend>Rule {i + 1}</legend>
          <label htmlFor={`inspector-rule-${i}-when`}>When</label>
          <textarea id={`inspector-rule-${i}-when`} value={r.when} placeholder="msg.total > 100" onChange={(e) => update(i, { when: e.target.value })} />
          <label htmlFor={`inspector-rule-${i}-topic`}>Topic</label>
          <select id={`inspector-rule-${i}-topic`} value={r.to} onChange={(e) => update(i, { to: e.target.value })}>
            {r.to === '' && <option value="">(pick a topic)</option>}
            {options(r.to)}
          </select>
          {!wired.has(r.to) && <p className="hint">Not wired: draw an edge from the router to its topic.</p>}
          <div className="buttons">
            <button disabled={i === 0} onClick={() => move(i, -1)}>
              Move up
            </button>
            <button disabled={i === data.rules.length - 1} onClick={() => move(i, 1)}>
              Move down
            </button>
            <button onClick={() => setRules(data.rules.filter((_, j) => j !== i))}>Remove</button>
          </div>
        </fieldset>
      ))}
      <button onClick={() => setRules([...data.rules, { when: '', to: topics[0]?.id ?? '' }])}>Add rule</button>
      <label htmlFor="inspector-default">Default</label>
      <select id="inspector-default" value={data.default} onChange={(e) => set({ default: e.target.value })}>
        <option value="">none: drop the record</option>
        {options(data.default)}
      </select>
      <p className="hint">
        First match wins. Without a default, records no rule matches are dropped and counted. Each condition is an expr-lang
        expression over msg (the value after the transform, if any) that yields true or false, e.g. {'msg.total > 100'}.
        Wiring the router to a topic adds a rule for it.
      </p>
    </>
  )
}
