import { z } from 'zod'

export const NODE_TYPES = ['producer', 'topic', 'consumer', 'transform'] as const
export type NodeType = (typeof NODE_TYPES)[number]

// Mirrors allowedEdges in ../../flow.go; the backend is the authority.
const ALLOWED: Record<NodeType, readonly NodeType[]> = {
  producer: ['topic'],
  topic: ['consumer'],
  consumer: ['topic', 'transform'],
  transform: ['topic'],
}

export function canConnect(source: string | undefined, target: string | undefined): boolean {
  return (ALLOWED[source as NodeType] ?? []).includes(target as NodeType)
}

// The file format is React Flow's own shape plus id and name (spec 4.1).
export const FlowSchema = z.object({
  id: z.string(),
  name: z.string(),
  nodes: z.array(
    z.object({
      id: z.string(),
      type: z.enum(NODE_TYPES),
      position: z.object({ x: z.number(), y: z.number() }),
      data: z.record(z.string(), z.unknown()),
    }),
  ),
  edges: z.array(z.object({ id: z.string(), source: z.string(), target: z.string() })),
  viewport: z.object({ x: z.number(), y: z.number(), zoom: z.number() }).optional(),
})
export type FlowFile = z.infer<typeof FlowSchema>

// What a node dragged from the palette starts with.
export function defaultData(type: NodeType): Record<string, unknown> {
  switch (type) {
    case 'producer':
      return { source: 'manual', key: '', value: '{"id": {{.Seq}}}' }
    case 'topic':
      return { name: '', partitions: 1, replication_factor: 1 }
    case 'consumer':
      return { group: '', auto_offset_reset: 'earliest', sink: { kind: 'log' } }
    case 'transform':
      return { expr: 'msg' }
  }
}

// Smallest unused "<type>-<n>"; ids must match the Go nodeIDRe.
export function nextId(type: NodeType, nodes: { id: string }[]): string {
  const used = new Set(nodes.map((n) => n.id))
  for (let n = 1; ; n++) {
    const id = `${type}-${n}`
    if (!used.has(id)) return id
  }
}
