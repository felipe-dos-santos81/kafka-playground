// A router's rules as the canvas shows and edits them, and the topic lookups the
// Inspector shares.
import type { Edge } from '@xyflow/react'
import type { RouterData, StudioNode } from '../nodes/types'

// The label on a router's edge to target: the rules that send there (#1, #2) and
// the default, each with its count while the flow runs (#1 · 80).
export function routeLabel(data: RouterData, target: string, branches?: number[]): string {
  const tos = [...data.rules.map((r) => r.to), data.default] // in the order of branches: the rules', then the default's
  return tos
    .flatMap((to, i) => {
      if (to !== target) return []
      const name = i < data.rules.length ? `#${i + 1}` : 'default'
      return [branches ? `${name} · ${branches[i] ?? 0}` : name]
    })
    .join(', ')
}

// The router's data once it is wired to topic: a new rule with a condition to fill
// in, unless a rule or the default already sends there (an edge deleted, then
// drawn again, finds its rule kept).
export function withRule(data: RouterData, topic: string): RouterData {
  if (data.default === topic || data.rules.some((r) => r.to === topic)) return data
  return { ...data, rules: [...data.rules, { when: '', to: topic }] }
}

// The topics a router has edges to, by name, for its rules' selects.
export function wiredTopics(id: string, nodes: StudioNode[], edges: Edge[]): { id: string; name: string }[] {
  return edges.flatMap((e) => {
    const t = e.source === id ? nodes.find((n) => n.id === e.target) : undefined
    return t?.type === 'topic' ? [{ id: t.id, name: topicName(t.id, t) }] : []
  })
}

// The name of the topic a consumer reads, '' while it reads none or the topic has
// no name yet (a consumer's retry and DLQ topics are named after it).
export function inputTopic(id: string, nodes: StudioNode[], edges: Edge[]): string {
  const from = edges.find((e) => e.target === id)?.source
  const topic = nodes.find((n) => n.id === from)
  return topic?.type === 'topic' ? topic.data.name : ''
}

// A topic as a router shows it: its name, or its node id while it has none (or
// is gone).
export const topicName = (id: string, topic?: { data: { name: string } } | null) => topic?.data.name || id
