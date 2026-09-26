// API service layer for FT-DOSS dashboard
import axios from 'axios'

const BASE_URL = import.meta.env.VITE_API_URL || '/api'

const api = axios.create({
  baseURL: BASE_URL,
  timeout: 10000,
  headers: {
    'X-Admin-API-Key': import.meta.env.VITE_ADMIN_API_KEY || 'dev-key',
  },
})

// ─── Types ───────────────────────────────────────────────────────────────────

export type NodeState = 'HEALTHY' | 'SUSPECT' | 'UNAVAILABLE' | 'DECOMMISSIONING' | 'RECOVERING'
export type ReplicaState = 'MISSING' | 'PRESENT' | 'STALE' | 'CORRUPT' | 'REPAIRING' | 'VALID'
export type RaftState = 'FOLLOWER' | 'CANDIDATE' | 'LEADER'
export type AlertSeverity = 'CRITICAL' | 'WARNING' | 'INFO'

export interface StorageNode {
  id: string
  address: string
  http_port: number
  zone: string
  rack: string
  region: string
  state: NodeState
  cluster_epoch: number
  disk_total: number
  disk_free: number
  disk_used: number
  object_count: number
  replica_count: number
  last_heartbeat: string
  joined_at: string
  weight: number
}

export interface ClusterMetrics {
  timestamp: string
  total_nodes: number
  healthy_nodes: number
  suspect_nodes: number
  unavailable_nodes: number
  total_objects: number
  total_size_bytes: number
  raw_capacity: number
  usable_capacity: number
  free_capacity: number
  degraded_objects: number
  corrupt_replicas: number
  pending_repairs: number
  degraded_placement_groups: number
  stale_replicas: number
  raft_leader_id: string
  raft_term: number
  raft_commit_index: number
  cluster_epoch: number
  total_reads: number
  successful_reads: number
  total_writes: number
  successful_writes: number
  read_latency_p50_ms: number
  read_latency_p99_ms: number
  write_latency_p50_ms: number
  write_latency_p99_ms: number
}

export interface RaftStatus {
  node_id: string
  state: RaftState
  term: number
  leader_id: string
  commit_index: number
  last_applied: number
  log_length: number
  election_count: number
}

export interface PlacementGroup {
  id: number
  nodes: string[]
  state: string
  object_count: number
  size_bytes: number
}

export interface RepairJob {
  id: string
  bucket: string
  key: string
  version_id: string
  target_node: string
  source_node: string
  state: ReplicaState
  reason: string
  priority: number
  created_at: string
  started_at?: string
  completed_at?: string
  error?: string
  bytes_copied: number
  total_bytes: number
}

export interface Alert {
  id: string
  severity: AlertSeverity
  title: string
  message: string
  node_id?: string
  created_at: string
  resolved_at?: string
  is_active: boolean
}

export interface ObjectMetadata {
  bucket: string
  key: string
  version_id: string
  version_sequence: number
  size: number
  checksum: string
  content_type: string
  state: string
  placement_group: number
  replica_nodes: string[]
  created_at: string
  modified_at: string
  is_tombstone: boolean
  etag: string
}

// ─── API Functions ───────────────────────────────────────────────────────────

export const getClusterMetrics = () =>
  api.get<ClusterMetrics>('/admin/cluster/metrics').then(r => r.data)

export const getNodes = () =>
  api.get<StorageNode[]>('/admin/nodes').then(r => r.data)

export const getNode = (nodeId: string) =>
  api.get<StorageNode>(`/admin/nodes/${nodeId}`).then(r => r.data)

export const getRaftStatus = () =>
  api.get<RaftStatus>('/admin/raft').then(r => r.data)

export const getPlacementGroups = () =>
  api.get<PlacementGroup[]>('/admin/placement-groups').then(r => r.data)

export const getRepairs = () =>
  api.get<RepairJob[]>('/admin/repairs').then(r => r.data)

export const getAlerts = () =>
  api.get<Alert[]>('/admin/alerts').then(r => r.data)

export const getClusterOverview = () =>
  api.get('/admin/cluster').then(r => r.data)

export const getRebalanceStatus = () =>
  api.get('/admin/rebalance').then(r => r.data)

export const getScrubs = () =>
  api.get('/admin/scrubs').then(r => r.data)

export const listObjects = (bucket: string, prefix = '', limit = 100) =>
  api.get<{ objects?: ObjectMetadata[]; bucket?: string; prefix?: string }>(`/buckets/${bucket}/list?prefix=${prefix}&limit=${limit}`).then(r => r.data)


// ─── Chaos Lab ───────────────────────────────────────────────────────────────

export const crashNode = (nodeId: string) =>
  api.post(`/admin/chaos/crash-node/${nodeId}`).then(r => r.data)

export const restartNode = (nodeId: string) =>
  api.post(`/admin/chaos/restart-node/${nodeId}`).then(r => r.data)

export const corruptReplica = (bucket: string, key: string, versionId: string) =>
  api.post('/admin/chaos/corrupt-replica', { bucket, key, version_id: versionId }).then(r => r.data)

export const addNode = (node: Partial<StorageNode>) =>
  api.post('/admin/chaos/add-node', node).then(r => r.data)

export const removeNode = (nodeId: string) =>
  api.post(`/admin/chaos/remove-node/${nodeId}`).then(r => r.data)

export const triggerRepair = () =>
  api.post('/admin/repairs/trigger').then(r => r.data)

export const triggerScrub = (nodeId: string) =>
  api.post(`/admin/scrubs/trigger/${nodeId}`).then(r => r.data)

export const getSSETicket = () =>
  api.post<{ token: string; expires_in: number }>('/admin/sse-token').then(r => r.data)

export interface PutObjectResult {
  bucket?: string
  key?: string
  version_id: string
  etag: string
  size: number
  checksum: string
  replica_nodes?: string[]
  placement_group?: number
  quorum_met?: boolean
}

// ─── Object API ──────────────────────────────────────────────────────────────

export const putObject = (bucket: string, key: string, data: string | Blob, contentType = 'application/octet-stream'): Promise<PutObjectResult> =>
  api.put<PutObjectResult>(`/buckets/${bucket}/objects/${key}`, data, {
    headers: { 'Content-Type': contentType }
  }).then(r => r.data)

export const getObject = (bucket: string, key: string) =>
  api.get(`/buckets/${bucket}/objects/${key}`, { responseType: 'arraybuffer' }).then(r => r.data)

export const getObjectText = async (bucket: string, key: string): Promise<string> => {
  const data = await getObject(bucket, key)
  const decoder = new TextDecoder('utf-8')
  return decoder.decode(data)
}

export const deleteObject = (bucket: string, key: string) =>
  api.delete(`/buckets/${bucket}/objects/${key}`).then(r => r.data)

export const createBucket = (bucket: string) =>
  api.put(`/buckets/${bucket}`).then(r => r.data)

// ─── Utilities ───────────────────────────────────────────────────────────────

export const formatBytes = (bytes: number): string => {
  if (bytes === 0) return '0 B'
  const k = 1024
  const sizes = ['B', 'KB', 'MB', 'GB', 'TB', 'PB']
  const i = Math.floor(Math.log(bytes) / Math.log(k))
  return parseFloat((bytes / Math.pow(k, i)).toFixed(1)) + ' ' + sizes[i]
}

export const formatNumber = (n: number): string => {
  if (n >= 1e9) return (n / 1e9).toFixed(1) + 'B'
  if (n >= 1e6) return (n / 1e6).toFixed(1) + 'M'
  if (n >= 1e3) return (n / 1e3).toFixed(1) + 'K'
  return n.toString()
}
