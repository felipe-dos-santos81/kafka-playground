import type { Node } from '@xyflow/react'

// Node.data per type; field names match the Go structs in ../../flow.go.
export type ProducerData = { source: 'manual' | 'timer'; interval_ms?: number; key: string; value: string }
export type TopicData = { name: string; partitions: number; replication_factor: number }
export type ConsumerData = {
  group: string
  auto_offset_reset: 'earliest' | 'latest'
  instances?: number
  sink: { kind: 'log' | 'http'; url?: string }
}
export type TransformData = { expr: string }

export type ProducerNode = Node<ProducerData, 'producer'>
export type TopicNode = Node<TopicData, 'topic'>
export type ConsumerNode = Node<ConsumerData, 'consumer'>
export type TransformNode = Node<TransformData, 'transform'>
export type StudioNode = ProducerNode | TopicNode | ConsumerNode | TransformNode
