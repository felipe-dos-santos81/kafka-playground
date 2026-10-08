// A router's rules as the canvas shows and edits them.
import type { RouterData } from '../nodes/types'

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
