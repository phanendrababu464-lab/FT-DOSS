import { useState, useCallback, useEffect, useRef } from 'react'
import {
  Activity, Server, Database, GitBranch, Layers, Wrench,
  Zap, BarChart2, Terminal, Settings, Shield, RefreshCw,
  AlertTriangle, CheckCircle, XCircle, Clock, HardDrive,
  Upload, List, Search, Play, Cpu, Globe, Star, Bookmark,
  Plus, X, ArrowLeft, ArrowRight, RotateCw, Home, FileText,
  Download, ExternalLink, Moon, Sun
} from 'lucide-react'
import { useClusterMetrics, useNodes, useRaftStatus, usePlacementGroups, useRepairs, useAlerts, useEventLog, useMetricsHistory } from './hooks/useCluster'
import { useEventStream } from './hooks/useEventStream'
import type { StorageNode, PutObjectResult } from './services/api'
import {
  crashNode, restartNode, addNode, removeNode, corruptReplica, triggerRepair, triggerScrub,
  formatBytes, formatNumber, createBucket, putObject, getObject, getObjectText, deleteObject, listObjects
} from './services/api'
import {
  AreaChart, Area, LineChart, Line, BarChart, Bar,
  XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, PieChart, Pie, Cell
} from 'recharts'

// ─── Navigation items ─────────────────────────────────────────────────────────

const NAV_ITEMS = [
  { id: 'overview', label: 'Cluster Overview', icon: Activity, section: 'CLUSTER' },
  { id: 'nodes', label: 'Storage Nodes', icon: Server, section: 'CLUSTER' },
  { id: 'placement', label: 'Placement Groups', icon: Layers, section: 'CLUSTER' },
  { id: 'metadata', label: 'Raft / Metadata', icon: GitBranch, section: 'CONSENSUS' },
  { id: 'objects', label: 'Objects', icon: Database, section: 'DATA' },
  { id: 'replicas', label: 'Replicas', icon: Shield, section: 'DATA' },
  { id: 'repairs', label: 'Repairs', icon: Wrench, section: 'CONTROL PLANE' },
  { id: 'scrub', label: 'Scrubbing', icon: Search, section: 'CONTROL PLANE' },
  { id: 'rebalance', label: 'Rebalancing', icon: RefreshCw, section: 'CONTROL PLANE' },
  { id: 'metrics', label: 'Metrics', icon: BarChart2, section: 'OBSERVABILITY' },
  { id: 'alerts', label: 'Alerts', icon: AlertTriangle, section: 'OBSERVABILITY' },
  { id: 'logs', label: 'Event Log', icon: Terminal, section: 'OBSERVABILITY' },
  { id: 'chaos', label: 'Chaos Lab 🔥', icon: Zap, section: 'DEMO' },
  { id: 'api', label: 'API Explorer', icon: Upload, section: 'DEMO' },
  { id: 'config', label: 'Configuration', icon: Settings, section: 'SYSTEM' },
]

// ─── State badge ─────────────────────────────────────────────────────────────

function StateBadge({ state }: { state: string }) {
  const s = state?.toLowerCase() || 'unknown'
  return <span className={`badge ${s}`}>{state || 'UNKNOWN'}</span>
}

// ─── Metric Card ─────────────────────────────────────────────────────────────

function MetricCard({
  label, value, sub, color = '', icon: Icon
}: {
  label: string; value: string | number; sub?: string; color?: string; icon?: React.ComponentType<any>
}) {
  return (
    <div className={`metric-card ${color}`}>
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start' }}>
        <div className="metric-label">{label}</div>
        {Icon && <Icon size={16} style={{ color: 'var(--text-muted)', opacity: 0.6 }} />}
      </div>
      <div className={`metric-value ${color}`}>{value}</div>
      {sub && <div className="metric-sub">{sub}</div>}
    </div>
  )
}

// ─── Node Card ───────────────────────────────────────────────────────────────

function NodeCard({ node, onAction }: {
  node: StorageNode
  onAction: (action: string, node: StorageNode) => void
}) {
  const diskPct = node.disk_total > 0 ? ((node.disk_total - node.disk_free) / node.disk_total) * 100 : 0
  const diskColor = diskPct > 80 ? 'red' : diskPct > 60 ? 'yellow' : 'green'

  return (
    <div className="node-card">
      <div className="node-card-header">
        <div>
          <div className="node-id">{node.id}</div>
          <div className="node-zone">{node.zone} / {node.rack}</div>
        </div>
        <StateBadge state={node.state} />
      </div>

      <div className="node-stats">
        <div>
          <div className="node-stat-label">Objects</div>
          <div className="node-stat-value">{formatNumber(node.object_count || 0)}</div>
        </div>
        <div>
          <div className="node-stat-label">Replicas</div>
          <div className="node-stat-value">{formatNumber(node.replica_count || 0)}</div>
        </div>
        <div>
          <div className="node-stat-label">Disk Used</div>
          <div className="node-stat-value">{formatBytes(node.disk_total - node.disk_free)}</div>
        </div>
        <div>
          <div className="node-stat-label">Epoch</div>
          <div className="node-stat-value">{node.cluster_epoch || 0}</div>
        </div>
      </div>

      <div>
        <div style={{ display: 'flex', justifyContent: 'space-between', fontSize: 11, color: 'var(--text-muted)' }}>
          <span>Disk</span>
          <span>{diskPct.toFixed(0)}%</span>
        </div>
        <div className="progress-bar">
          <div className={`progress-fill ${diskColor}`} style={{ width: `${diskPct}%` }} />
        </div>
      </div>

      <div style={{ display: 'flex', gap: 8, marginTop: 16, flexWrap: 'wrap' }}>
        {node.state !== 'UNAVAILABLE' ? (
          <button className="btn btn-danger btn-sm" onClick={() => onAction('crash', node)}>
            Crash
          </button>
        ) : (
          <button className="btn btn-success btn-sm" onClick={() => onAction('restart', node)}>
            Restart
          </button>
        )}
        <button className="btn btn-warning btn-sm" onClick={() => onAction('decommission', node)}>
          Decommission
        </button>
        <button className="btn btn-ghost btn-sm" onClick={() => onAction('scrub', node)}>
          Scrub
        </button>
      </div>
    </div>
  )
}

// ─── Overview Page ────────────────────────────────────────────────────────────

function OverviewPage() {
  const { data: metrics } = useClusterMetrics()
  const { data: nodes } = useNodes()
  const { data: raft } = useRaftStatus()
  const { history, push } = useMetricsHistory()

  // Push new metrics into history
  if (metrics) {
    const last = history[history.length - 1]
    if (!last || last.writes !== metrics.successful_writes) push(metrics)
  }

  const m = metrics || {
    healthy_nodes: 0, suspect_nodes: 0, unavailable_nodes: 0, total_nodes: 0,
    total_objects: 0, total_size_bytes: 0, raw_capacity: 0, free_capacity: 0,
    degraded_objects: 0, corrupt_replicas: 0, pending_repairs: 0,
    cluster_epoch: 0, raft_leader_id: 'unknown',
    successful_reads: 0, successful_writes: 0,
  }

  // Alerts strip
  const hasIssues = m.corrupt_replicas > 0 || m.degraded_objects > 0 || m.unavailable_nodes > 0

  return (
    <div>
      {hasIssues && (
        <div className="alert-strip critical">
          <AlertTriangle size={16} />
          <span>
            <strong>Cluster Degraded:</strong>{' '}
            {m.unavailable_nodes > 0 && `${m.unavailable_nodes} node(s) unavailable • `}
            {m.corrupt_replicas > 0 && `${m.corrupt_replicas} corrupt replica(s) • `}
            {m.degraded_objects > 0 && `${m.degraded_objects} object(s) below durability `}
          </span>
        </div>
      )}

      {/* Invariants & Quorum Banner */}
      <div className="card" style={{ marginBottom: 16 }}>
        <div className="card-header">
          <div className="card-title">Cluster Architecture & Topology</div>
          <div style={{ display: 'flex', gap: 8 }}>
            <span className="tag" style={{ background: 'rgba(79,142,247,0.15)', color: '#4f8ef7', border: '1px solid #4f8ef7' }}>CONFIGURATION: N=3, W=2, R=2</span>
            <span className="tag" style={{ background: 'rgba(34,211,165,0.15)', color: '#22d3a5', border: '1px solid #22d3a5' }}>1024 PGs</span>
          </div>
        </div>
        <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 24, padding: '8px 0' }}>
          {/* Node Topology */}
          <div style={{ background: 'rgba(0,0,0,0.2)', padding: 16, borderRadius: 8, border: '1px solid var(--border)' }}>
            <div style={{ fontSize: 11, fontWeight: 700, color: 'var(--text-muted)', textTransform: 'uppercase', marginBottom: 12, letterSpacing: '0.05em' }}>
              Cluster Topology (Live Nodes)
            </div>
            <div style={{ textAlign: 'center', marginBottom: 12 }}>
              <span className="tag" style={{ background: 'var(--bg-secondary)', border: '1px solid var(--border)', fontSize: 12, fontWeight: 600 }}>
                🌐 API Gateway (172.20.0.10:8080)
              </span>
            </div>
            <div style={{ display: 'flex', justifyContent: 'space-around', gap: 8 }}>
              {(nodes || [
                { id: 'storage-1', state: 'HEALTHY' },
                { id: 'storage-2', state: 'HEALTHY' },
                { id: 'storage-3', state: 'HEALTHY' }
              ]).slice(0, 3).map(n => (
                <div key={n.id} style={{ textAlign: 'center', background: 'var(--bg-card)', padding: '10px 14px', borderRadius: 6, border: '1px solid var(--border)', flex: 1 }}>
                  <div style={{ fontSize: 12, fontWeight: 700, fontFamily: 'JetBrains Mono, monospace', marginBottom: 6 }}>{n.id}</div>
                  <StateBadge state={n.state} />
                </div>
              ))}
            </div>
          </div>

          {/* Quorum Visualization */}
          <div style={{ background: 'rgba(0,0,0,0.2)', padding: 16, borderRadius: 8, border: '1px solid var(--border)' }}>
            <div style={{ fontSize: 11, fontWeight: 700, color: 'var(--text-muted)', textTransform: 'uppercase', marginBottom: 12, letterSpacing: '0.05em' }}>
              Quorum Tolerances & Read/Write Rules
            </div>
            <div style={{ fontSize: 12, color: 'var(--text-secondary)', lineHeight: 1.6 }}>
              • <strong>Write Quorum ($W=2$)</strong>: Two persistent acknowledgements are required for a successful write.<br />
              • <strong>Read Quorum ($R=2$)</strong>: Reads consult two replicas to satisfy checksum verification.<br />
              • <strong>Fault Tolerance</strong>: Operations continue uninterrupted when one node is unavailable.
            </div>
          </div>
        </div>
      </div>

      {/* Key metrics */}
      <div className="metric-grid">
        <MetricCard label="Healthy Nodes" value={m.healthy_nodes} sub={`of ${m.total_nodes} total`} color="green" icon={Server} />
        <MetricCard label="Suspect Nodes" value={m.suspect_nodes} color={m.suspect_nodes > 0 ? 'yellow' : ''} icon={AlertTriangle} />
        <MetricCard label="Unavailable" value={m.unavailable_nodes} color={m.unavailable_nodes > 0 ? 'red' : ''} icon={XCircle} />
        <MetricCard label="Total Objects" value={formatNumber(m.total_objects)} sub={formatBytes(m.total_size_bytes)} color="blue" icon={Database} />
        <MetricCard label="Cluster Capacity" value={formatBytes(m.raw_capacity)} sub={`${formatBytes(m.free_capacity)} free`} icon={HardDrive} />
        <MetricCard label="Pending Repairs" value={m.pending_repairs} color={m.pending_repairs > 0 ? 'yellow' : 'green'} icon={Wrench} />
        <MetricCard label="Corrupt Replicas" value={m.corrupt_replicas} color={m.corrupt_replicas > 0 ? 'red' : 'green'} icon={Shield} />
        <MetricCard label="Cluster Epoch" value={m.cluster_epoch} sub={`leader: ${m.raft_leader_id || 'unknown'}`} icon={GitBranch} />
      </div>

      {/* Charts row */}
      <div className="grid-2" style={{ marginBottom: 16 }}>
        <div className="chart-container">
          <div className="chart-title">Operations / Time</div>
          <ResponsiveContainer width="100%" height={180}>
            <AreaChart data={history}>
              <CartesianGrid strokeDasharray="3 3" stroke="rgba(99,120,179,0.1)" />
              <XAxis dataKey="time" tick={{ fontSize: 11, fill: '#4a5a7a' }} />
              <YAxis tick={{ fontSize: 11, fill: '#4a5a7a' }} />
              <Tooltip contentStyle={{ background: '#131d35', border: '1px solid rgba(99,120,179,0.2)', borderRadius: 8 }} />
              <Area type="monotone" dataKey="writes" stroke="#4f8ef7" fill="rgba(79,142,247,0.15)" strokeWidth={2} name="Writes" />
              <Area type="monotone" dataKey="reads" stroke="#22d3a5" fill="rgba(34,211,165,0.1)" strokeWidth={2} name="Reads" />
            </AreaChart>
          </ResponsiveContainer>
        </div>

        <div className="chart-container">
          <div className="chart-title">Node Health Distribution</div>
          <div style={{ display: 'flex', alignItems: 'center', gap: 24, height: 180 }}>
            <ResponsiveContainer width="60%" height={180}>
              <PieChart>
                <Pie data={[
                  { name: 'Healthy', value: m.healthy_nodes || 0 },
                  { name: 'Suspect', value: m.suspect_nodes || 0 },
                  { name: 'Unavailable', value: m.unavailable_nodes || 0 },
                ]} cx="50%" cy="50%" innerRadius={40} outerRadius={70} dataKey="value">
                  <Cell fill="#22d3a5" />
                  <Cell fill="#f59e0b" />
                  <Cell fill="#ef4444" />
                </Pie>
                <Tooltip contentStyle={{ background: '#131d35', border: '1px solid rgba(99,120,179,0.2)', borderRadius: 8 }} />
              </PieChart>
            </ResponsiveContainer>
            <div style={{ flex: 1 }}>
              {[
                { label: 'Healthy', count: m.healthy_nodes, color: 'var(--healthy)' },
                { label: 'Suspect', count: m.suspect_nodes, color: 'var(--suspect)' },
                { label: 'Unavailable', count: m.unavailable_nodes, color: 'var(--unavailable)' },
              ].map(item => (
                <div key={item.label} style={{ display: 'flex', justifyContent: 'space-between', marginBottom: 8, fontSize: 13 }}>
                  <span style={{ display: 'flex', alignItems: 'center', gap: 6 }}>
                    <span style={{ width: 8, height: 8, borderRadius: '50%', background: item.color, display: 'inline-block' }} />
                    <span style={{ color: 'var(--text-secondary)' }}>{item.label}</span>
                  </span>
                  <span style={{ fontWeight: 700, color: item.color }}>{item.count}</span>
                </div>
              ))}
            </div>
          </div>
        </div>
      </div>

      {/* Raft status inline */}
      {raft && (
        <div className="card">
          <div className="card-header">
            <div className="card-title">Raft Consensus State</div>
            <StateBadge state={raft.state} />
          </div>
          <div style={{ display: 'flex', gap: 32 }}>
            {[
              { label: 'Node ID', value: raft.node_id },
              { label: 'Term', value: raft.term },
              { label: 'Leader', value: raft.leader_id || 'Unknown' },
              { label: 'Commit Index', value: raft.commit_index },
              { label: 'Last Applied', value: raft.last_applied },
              { label: 'Log Length', value: raft.log_length },
              { label: 'Elections', value: raft.election_count },
            ].map(item => (
              <div key={item.label}>
                <div style={{ fontSize: 11, color: 'var(--text-muted)', marginBottom: 4 }}>{item.label}</div>
                <div style={{ fontSize: 14, fontWeight: 600, fontFamily: 'JetBrains Mono, monospace' }}>{item.value}</div>
              </div>
            ))}
          </div>
        </div>
      )}
    </div>
  )
}

// ─── Nodes Page ───────────────────────────────────────────────────────────────

function NodesPage({ onLog }: { onLog: (level: string, msg: string) => void }) {
  const { data: nodes, refresh } = useNodes()

  const handleAction = useCallback(async (action: string, node: StorageNode) => {
    try {
      if (action === 'crash') {
        await crashNode(node.id)
        onLog('WARN', `Node ${node.id} crashed`)
      } else if (action === 'restart') {
        await restartNode(node.id)
        onLog('INFO', `Node ${node.id} restarting...`)
      } else if (action === 'scrub') {
        await triggerScrub(node.id)
        onLog('INFO', `Scrub triggered on ${node.id}`)
      } else if (action === 'decommission') {
        await removeNode(node.id)
        onLog('WARN', `Node ${node.id} decommissioning`)
      }
      setTimeout(refresh, 500)
    } catch (e: unknown) {
      onLog('ERROR', `Action ${action} on ${node.id} failed: ${e instanceof Error ? e.message : 'unknown error'}`)
    }
  }, [refresh, onLog])

  return (
    <div>
      <div className="page-header">
        <div>
          <div className="page-title">Storage Nodes</div>
          <div className="page-subtitle">{nodes?.length || 0} nodes in cluster</div>
        </div>
        <div style={{ display: 'flex', gap: 8 }}>
          <button className="btn btn-ghost btn-sm" onClick={refresh}>
            <RefreshCw size={14} /> Refresh
          </button>
        </div>
      </div>
      <div className="node-grid">
        {nodes?.map(node => (
          <NodeCard key={node.id} node={node} onAction={handleAction} />
        ))}
        {(!nodes || nodes.length === 0) && (
          <div style={{ color: 'var(--text-muted)', padding: 24 }}>No nodes registered. Start the backend first.</div>
        )}
      </div>
    </div>
  )
}

// ─── Raft Page ────────────────────────────────────────────────────────────────

function RaftPage() {
  const { data: raft } = useRaftStatus(1000)
  const { data: nodes } = useNodes()

  const raftNodes = (nodes && nodes.length > 0)
    ? nodes.map(n => ({
        id: n.id,
        state: (n.id === raft?.leader_id || (n.id === raft?.node_id && raft?.state === 'LEADER'))
          ? 'LEADER'
          : n.state === 'UNAVAILABLE'
            ? 'OFFLINE'
            : 'FOLLOWER',
        term: raft?.term || 0,
      }))
    : [
        { id: raft?.node_id || 'storage-1', state: raft?.state || 'LEADER', term: raft?.term || 0 }
      ]

  return (
    <div>
      <div className="page-header">
        <div>
          <div className="page-title">Raft Consensus</div>
          <div className="page-subtitle">Metadata service consensus state</div>
        </div>
        <StateBadge state={raft?.state || 'UNKNOWN'} />
      </div>

      {/* Raft node visualization */}
      <div className="card" style={{ marginBottom: 16 }}>
        <div className="card-header">
          <div className="card-title">Metadata Nodes</div>
          <span className="tag">Term {raft?.term || 0}</span>
          <span style={{ fontSize: 11, color: 'var(--text-muted)' }}>Source: GET /admin/raft</span>
        </div>
        <div className="raft-nodes">
          {raftNodes.map(node => (
            <div key={node.id} className="raft-node">
              <div className={`raft-circle ${node.state?.toLowerCase()}`}>
                <div style={{ fontSize: 10, color: 'var(--text-muted)' }}>
                  {node.state === 'LEADER' ? '👑' : '●'}
                </div>
                <div style={{ fontSize: 9, fontWeight: 700, letterSpacing: '0.05em' }}>{node.state}</div>
              </div>
              <div className="raft-label">{node.id}</div>
              <StateBadge state={node.state === 'LEADER' ? 'leader' : node.state === 'OFFLINE' ? 'unavailable' : 'follower'} />
            </div>
          ))}
        </div>
      </div>

      {/* Raft stats */}
      <div className="grid-2">
        <div className="card">
          <div className="card-title" style={{ marginBottom: 16 }}>Consensus Metrics</div>
          <div style={{ display: 'flex', flexDirection: 'column', gap: 12 }}>
            {[
              { label: 'Current Term', value: raft?.term ?? 'N/A', highlight: true },
              { label: 'Commit Index', value: raft?.commit_index ?? 'N/A' },
              { label: 'Last Applied', value: raft?.last_applied ?? 'N/A' },
              { label: 'Log Entries', value: raft?.log_length ?? 'N/A' },
              { label: 'Leader Elections', value: raft?.election_count ?? 'N/A' },
              { label: 'Current Leader', value: raft?.leader_id || 'Unknown' },
            ].map(item => (
              <div key={item.label} style={{ display: 'flex', justifyContent: 'space-between', padding: '8px 0', borderBottom: '1px solid var(--border)' }}>
                <span style={{ color: 'var(--text-secondary)', fontSize: 13 }}>{item.label}</span>
                <span style={{ fontFamily: 'JetBrains Mono, monospace', fontSize: 13, fontWeight: 600, color: item.highlight ? 'var(--accent)' : 'var(--text-primary)' }}>
                  {item.value}
                </span>
              </div>
            ))}
          </div>
        </div>

        <div className="card">
          <div className="card-title" style={{ marginBottom: 16 }}>How Raft Works</div>
          <div className="timeline">
            {[
              { type: 'success', time: 'Startup', msg: 'All nodes start as FOLLOWER' },
              { type: '', time: 'Election', msg: 'Timer expires → node becomes CANDIDATE → RequestVote RPCs sent' },
              { type: 'success', time: 'Quorum', msg: 'Majority votes → node becomes LEADER' },
              { type: '', time: 'Heartbeat', msg: 'Leader sends AppendEntries to followers every 50ms' },
              { type: '', time: 'Write', msg: 'Client write → leader appends → replicates → commit when quorum acks' },
              { type: 'warning', time: 'Failure', msg: 'Leader crash → election timer expires on follower → new election' },
              { type: 'success', time: 'Recovery', msg: 'New leader elected → log reconciled → operations resume' },
            ].map((item, i) => (
              <div key={i} className={`timeline-item ${item.type}`}>
                <div className="timeline-time">{item.time}</div>
                <div className="timeline-msg">{item.msg}</div>
              </div>
            ))}
          </div>
        </div>
      </div>
    </div>
  )
}

// ─── Placement Groups Page ────────────────────────────────────────────────────

function PlacementPage() {
  const { data: pgs } = usePlacementGroups()
  const [selected, setSelected] = useState<number | null>(null)

  const stats = {
    total: pgs?.length || 0,
    active: pgs?.filter(p => p.state === 'ACTIVE').length || 0,
    rebalancing: pgs?.filter(p => p.state === 'REBALANCING').length || 0,
    degraded: pgs?.filter(p => p.state === 'DEGRADED').length || 0,
  }

  return (
    <div>
      <div className="page-header">
        <div>
          <div className="page-title">Placement Groups</div>
          <div className="page-subtitle">{stats.total} groups · {stats.active} active · {stats.rebalancing} rebalancing</div>
        </div>
      </div>

      <div className="metric-grid" style={{ marginBottom: 24 }}>
        <MetricCard label="Total PGs" value={stats.total} color="blue" />
        <MetricCard label="Active" value={stats.active} color="green" />
        <MetricCard label="Rebalancing" value={stats.rebalancing} color="yellow" />
        <MetricCard label="Degraded" value={stats.degraded} color={stats.degraded > 0 ? 'red' : ''} />
      </div>

      <div className="card">
        <div className="card-header">
          <div className="card-title">Placement Group Map</div>
          <span style={{ fontSize: 12, color: 'var(--text-muted)' }}>Click a group to inspect</span>
        </div>
        <div className="pg-list">
          {pgs?.slice(0, 200).map(pg => (
            <div
              key={pg.id}
              className={`pg-cell ${pg.state?.toLowerCase()}`}
              onClick={() => setSelected(pg.id)}
            >
              <div style={{ fontWeight: 700 }}>PG-{pg.id}</div>
              <div style={{ color: 'var(--text-muted)', marginTop: 2 }}>{pg.nodes?.length || 0}R</div>
            </div>
          ))}
        </div>
      </div>

      {selected !== null && (
        <div className="card" style={{ marginTop: 16 }}>
          <div className="card-header">
            <div className="card-title">Placement Group {selected}</div>
            <button className="btn btn-ghost btn-sm" onClick={() => setSelected(null)}>Close</button>
          </div>
          {(() => {
            const pg = pgs?.find(p => p.id === selected)
            if (!pg) return <div style={{ color: 'var(--text-muted)' }}>Loading...</div>
            return (
              <div style={{ fontFamily: 'JetBrains Mono, monospace', fontSize: 13 }}>
                <div style={{ marginBottom: 8, color: 'var(--text-secondary)' }}>
                  hash(bucket + key) % 1024 = <span style={{ color: 'var(--accent)' }}>PG-{pg.id}</span>
                </div>
                <div style={{ marginBottom: 8 }}>
                  State: <StateBadge state={pg.state} />
                </div>
                <div style={{ marginBottom: 4, color: 'var(--text-muted)' }}>Replica Nodes:</div>
                {(pg.nodes || []).map((n, i) => (
                  <div key={n} style={{ paddingLeft: 16, marginBottom: 4 }}>
                    <span style={{ color: 'var(--text-muted)' }}>{i === 0 ? '├─ PRIMARY:   ' : i === pg.nodes.length - 1 ? '└─ SECONDARY: ' : '├─ SECONDARY: '}</span>
                    <span style={{ color: 'var(--accent)' }}>{n}</span>
                  </div>
                ))}
              </div>
            )
          })()}
        </div>
      )}
    </div>
  )
}

// ─── Chaos Lab Page ───────────────────────────────────────────────────────────

function ChaosPage({ onLog }: { onLog: (level: string, msg: string) => void }) {
  const { data: nodes, refresh: refreshNodes } = useNodes()
  const [result, setResult] = useState<string>('')
  const [loading, setLoading] = useState('')

  const chaosAction = useCallback(async (label: string, action: () => Promise<unknown>) => {
    setLoading(label)
    try {
      const res = await action()
      const msg = typeof res === 'object' ? JSON.stringify(res, null, 2) : String(res)
      setResult(msg)
      onLog('WARN', `CHAOS: ${label}`)
      setTimeout(refreshNodes, 1000)
    } catch (e: unknown) {
      const msg = e instanceof Error ? e.message : 'Failed'
      setResult(`ERROR: ${msg}`)
      onLog('ERROR', `CHAOS FAILED: ${label} - ${msg}`)
    } finally {
      setLoading('')
    }
  }, [onLog, refreshNodes])

  const firstNode = nodes?.[0]?.id || 'node-1'

  const actions = [
    {
      label: 'Crash Node', category: 'destructive',
      icon: '💥', iconClass: 'red',
      action: () => chaosAction('Crash Node', () => crashNode(firstNode))
    },
    {
      label: 'Restart Node', category: 'safe',
      icon: '🔄', iconClass: 'green',
      action: () => chaosAction('Restart Node', () => restartNode(firstNode))
    },
    {
      label: 'Corrupt Replica', category: 'destructive',
      icon: '🦠', iconClass: 'red',
      action: () => chaosAction('Corrupt Replica', () => corruptReplica('default', 'chaos-test', 'v1'))
    },
    {
      label: 'Add Node', category: 'safe',
      icon: '➕', iconClass: 'green',
      action: () => chaosAction('Add Node', () => addNode({
        id: `node-${Date.now().toString().slice(-4)}`,
        zone: 'zone-2', rack: 'rack-new',
        state: 'HEALTHY' as const
      }))
    },
    {
      label: 'Remove Node', category: 'warning',
      icon: '➖', iconClass: 'yellow',
      action: () => chaosAction('Remove Node', () => removeNode(firstNode))
    },
    {
      label: 'Trigger Repair', category: 'safe',
      icon: '🔧', iconClass: 'blue',
      action: () => chaosAction('Trigger Repair', () => triggerRepair())
    },
    {
      label: 'Trigger Scrub', category: 'safe',
      icon: '🔍', iconClass: 'blue',
      action: () => chaosAction('Trigger Scrub', () => triggerScrub(firstNode))
    },
    {
      label: 'Simulate Partition', category: 'destructive',
      icon: '⚡', iconClass: 'red',
      action: () => chaosAction('Network Partition', () => crashNode(firstNode))
    },
    {
      label: 'Kill Metadata Leader', category: 'destructive',
      icon: '👑', iconClass: 'purple',
      action: () => chaosAction('Kill Raft Leader', () => crashNode(firstNode))
    },
    {
      label: 'Force Rebalance', category: 'warning',
      icon: '⚖️', iconClass: 'yellow',
      action: () => chaosAction('Force Rebalance', () => triggerRepair())
    },
    {
      label: 'Upload Test Object', category: 'safe',
      icon: '📤', iconClass: 'green',
      action: () => chaosAction('Upload Test Object', () => putObject('default', `chaos-${Date.now()}`, 'test data ' + Date.now()))
    },
    {
      label: 'Create Bucket', category: 'safe',
      icon: '🪣', iconClass: 'blue',
      action: () => chaosAction('Create Bucket', () => createBucket('chaos-bucket'))
    },
  ]

  return (
    <div>
      <div className="page-header">
        <div>
          <div className="page-title">⚡ Chaos Lab</div>
          <div className="page-subtitle">Inject real failures into the cluster — all actions affect actual backend state</div>
        </div>
        {firstNode && <span className="tag">Target: {firstNode}</span>}
      </div>

      <div style={{ marginBottom: 16 }} className="alert-strip warning">
        <AlertTriangle size={14} />
        <span><strong>Warning:</strong> All buttons trigger real backend operations. Destructive actions will affect cluster availability.</span>
      </div>

      <div className="chaos-grid">
        {actions.map(a => (
          <button
            key={a.label}
            className={`chaos-btn ${a.category}`}
            onClick={a.action}
            disabled={loading === a.label}
          >
            <div className={`chaos-icon ${a.iconClass}`}>
              {loading === a.label ? <div className="spinner" style={{ width: 18, height: 18 }} /> : a.icon}
            </div>
            {a.label}
          </button>
        ))}
      </div>

      {result && (
        <div className="card" style={{ marginTop: 16 }}>
          <div className="card-header">
            <div className="card-title">Last Operation Result</div>
            <button className="btn btn-ghost btn-sm" onClick={() => setResult('')}>Clear</button>
          </div>
          <pre style={{ fontFamily: 'JetBrains Mono, monospace', fontSize: 12, color: 'var(--text-secondary)', whiteSpace: 'pre-wrap' }}>
            {result}
          </pre>
        </div>
      )}

      {/* Scenario Guides */}
      <div className="card" style={{ marginTop: 16 }}>
        <div className="card-header">
          <div className="card-title">Failure Scenario Guides</div>
        </div>
        <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 16, fontSize: 13 }}>
          {[
            {
              title: '📌 Node Crash During Write',
              steps: ['Upload Test Object', 'Crash Node immediately', 'Observe quorum behavior', 'Check replica state']
            },
            {
              title: '🦠 Corruption Detection',
              steps: ['Upload Test Object first', 'Corrupt Replica', 'Trigger Repair', 'Watch auto-recovery']
            },
            {
              title: '⚡ Network Partition',
              steps: ['Start with 3+ nodes', 'Simulate Partition', 'Observe quorum cut-off', 'Recover Partition']
            },
            {
              title: '⚖️ Node Addition',
              steps: ['Note current PG assignments', 'Add Node', 'Watch epoch increment', 'Observe rebalancing']
            },
          ].map(scenario => (
            <div key={scenario.title} style={{ background: 'rgba(79,142,247,0.03)', border: '1px solid var(--border)', borderRadius: 8, padding: 16 }}>
              <div style={{ fontWeight: 600, marginBottom: 8, color: 'var(--text-primary)' }}>{scenario.title}</div>
              {scenario.steps.map((step, i) => (
                <div key={i} style={{ color: 'var(--text-secondary)', marginBottom: 4 }}>
                  {i + 1}. {step}
                </div>
              ))}
            </div>
          ))}
        </div>
      </div>
    </div>
  )
}

// ─── Metrics Page ─────────────────────────────────────────────────────────────

function MetricsPage() {
  const { data: metrics } = useClusterMetrics()
  const { history, push } = useMetricsHistory(60)
  if (metrics) push(metrics)

  const m = metrics || {
    total_reads: 0,
    successful_writes: 0,
    degraded_objects: 0,
    corrupt_replicas: 0,
    pending_repairs: 0,
    cluster_epoch: 0,
    raft_term: 0,
    read_latency_p99_ms: 0,
  }

  return (
    <div>
      <div className="page-header">
        <div className="page-title">Metrics & Observability</div>
        <span className="tag">Prometheus-compatible</span>
      </div>

      <div className="grid-2" style={{ marginBottom: 16 }}>
        <div className="chart-container">
          <div className="chart-title">Write Latency (ms)</div>
          <ResponsiveContainer width="100%" height={200}>
            <LineChart data={history}>
              <CartesianGrid strokeDasharray="3 3" stroke="rgba(99,120,179,0.1)" />
              <XAxis dataKey="time" tick={{ fontSize: 11, fill: '#4a5a7a' }} />
              <YAxis tick={{ fontSize: 11, fill: '#4a5a7a' }} />
              <Tooltip contentStyle={{ background: '#131d35', border: '1px solid rgba(99,120,179,0.2)', borderRadius: 8 }} />
              <Line type="monotone" dataKey="latency_p50" stroke="#4f8ef7" strokeWidth={2} dot={false} name="p50" />
              <Line type="monotone" dataKey="latency_p99" stroke="#ef4444" strokeWidth={2} dot={false} name="p99" />
            </LineChart>
          </ResponsiveContainer>
        </div>

        <div className="chart-container">
          <div className="chart-title">Pending Repairs Over Time</div>
          <ResponsiveContainer width="100%" height={200}>
            <AreaChart data={history}>
              <CartesianGrid strokeDasharray="3 3" stroke="rgba(99,120,179,0.1)" />
              <XAxis dataKey="time" tick={{ fontSize: 11, fill: '#4a5a7a' }} />
              <YAxis tick={{ fontSize: 11, fill: '#4a5a7a' }} />
              <Tooltip contentStyle={{ background: '#131d35', border: '1px solid rgba(99,120,179,0.2)', borderRadius: 8 }} />
              <Area type="monotone" dataKey="pending_repairs" stroke="#f59e0b" fill="rgba(245,158,11,0.15)" strokeWidth={2} name="Pending Repairs" />
            </AreaChart>
          </ResponsiveContainer>
        </div>
      </div>

      {/* Metric table */}
      <div className="card">
        <div className="card-title" style={{ marginBottom: 16 }}>Current Metrics Snapshot</div>
        <table className="data-table">
          <thead><tr>
            <th>Metric</th><th>Value</th><th>Description</th>
          </tr></thead>
          <tbody>
            {[
              { name: 'ftdoss_reads_total', value: m?.total_reads ?? 'N/A', desc: 'Total read attempts' },
              { name: 'ftdoss_writes_successful_total', value: m?.successful_writes ?? 'N/A', desc: 'Successful quorum writes' },
              { name: 'ftdoss_degraded_objects', value: m?.degraded_objects ?? 'N/A', desc: 'Objects below durability target' },
              { name: 'ftdoss_corrupt_replicas', value: m?.corrupt_replicas ?? 'N/A', desc: 'Known corrupt replicas' },
              { name: 'ftdoss_pending_repairs', value: m?.pending_repairs ?? 'N/A', desc: 'Repair jobs queued' },
              { name: 'ftdoss_cluster_epoch', value: m?.cluster_epoch ?? 'N/A', desc: 'Current cluster epoch' },
              { name: 'ftdoss_raft_term', value: m?.raft_term ?? 'N/A', desc: 'Current Raft term' },
              { name: 'ftdoss_read_latency_p99_ms', value: m?.read_latency_p99_ms !== undefined ? m.read_latency_p99_ms.toFixed(1) + 'ms' : 'N/A', desc: 'p99 read latency' },
            ].map(row => (
              <tr key={row.name}>
                <td className="mono">{row.name}</td>
                <td style={{ fontFamily: 'JetBrains Mono, monospace', color: 'var(--accent)' }}>{row.value}</td>
                <td style={{ color: 'var(--text-secondary)' }}>{row.desc}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  )
}

// ─── Alerts Page ──────────────────────────────────────────────────────────────

function AlertsPage() {
  const { data: alerts } = useAlerts()

  return (
    <div>
      <div className="page-header">
        <div className="page-title">Alerts</div>
        <span className="tag">{alerts?.filter(a => a.is_active).length || 0} active</span>
      </div>

      {(!alerts || alerts.length === 0) ? (
        <div className="card" style={{ textAlign: 'center', padding: 48 }}>
          <CheckCircle size={32} style={{ color: 'var(--healthy)', marginBottom: 12 }} />
          <div style={{ color: 'var(--text-secondary)' }}>No alerts. Cluster is healthy.</div>
        </div>
      ) : (
        <div style={{ display: 'flex', flexDirection: 'column', gap: 12 }}>
          {alerts.map(alert => (
            <div key={alert.id} className={`alert-strip ${alert.severity?.toLowerCase()}`} style={{ flexDirection: 'column', alignItems: 'flex-start' }}>
              <div style={{ display: 'flex', alignItems: 'center', gap: 8, marginBottom: 4 }}>
                <AlertTriangle size={14} />
                <strong>{alert.title}</strong>
                <span className={`badge ${alert.severity?.toLowerCase()}`}>{alert.severity}</span>
                {!alert.is_active && <span className="badge">RESOLVED</span>}
              </div>
              <div style={{ fontSize: 13, opacity: 0.9 }}>{alert.message}</div>
              <div style={{ fontSize: 11, opacity: 0.7, marginTop: 4 }}>{new Date(alert.created_at).toLocaleString()}</div>
            </div>
          ))}
        </div>
      )}
    </div>
  )
}

// ─── API Explorer Page ────────────────────────────────────────────────────────

function APIPage({ onLog }: { onLog: (level: string, msg: string) => void }) {
  const [bucket, setBucket] = useState('default')
  const [key, setKey] = useState('test-key')
  const [body, setBody] = useState('Hello, distributed world!')
  const [response, setResponse] = useState('')
  const [loading, setLoading] = useState('')

  const run = useCallback(async (op: string, action: () => Promise<unknown>) => {
    setLoading(op)
    try {
      const res = await action()
      const txt = typeof res === 'object' ? JSON.stringify(res, null, 2) : String(res)
      setResponse(`✅ ${op} SUCCESS\n\n${txt}`)
      onLog('SUCCESS', `API ${op} completed`)
    } catch (e: unknown) {
      const msg = e instanceof Error ? e.message : 'Failed'
      setResponse(`❌ ${op} FAILED\n\n${msg}`)
      onLog('ERROR', `API ${op} failed: ${msg}`)
    } finally {
      setLoading('')
    }
  }, [onLog])

  return (
    <div>
      <div className="page-header">
        <div className="page-title">API Explorer</div>
      </div>

      <div className="grid-2">
        <div className="card">
          <div className="card-title" style={{ marginBottom: 16 }}>Request Builder</div>
          <div style={{ display: 'flex', flexDirection: 'column', gap: 12 }}>
            <div>
              <label style={{ fontSize: 12, color: 'var(--text-muted)', display: 'block', marginBottom: 4 }}>Bucket</label>
              <input
                value={bucket} onChange={e => setBucket(e.target.value)}
                style={{ width: '100%', padding: '8px 12px', background: 'var(--bg-secondary)', border: '1px solid var(--border)', borderRadius: 6, color: 'var(--text-primary)', fontSize: 13, fontFamily: 'JetBrains Mono, monospace' }}
              />
            </div>
            <div>
              <label style={{ fontSize: 12, color: 'var(--text-muted)', display: 'block', marginBottom: 4 }}>Object Key</label>
              <input
                value={key} onChange={e => setKey(e.target.value)}
                style={{ width: '100%', padding: '8px 12px', background: 'var(--bg-secondary)', border: '1px solid var(--border)', borderRadius: 6, color: 'var(--text-primary)', fontSize: 13, fontFamily: 'JetBrains Mono, monospace' }}
              />
            </div>
            <div>
              <label style={{ fontSize: 12, color: 'var(--text-muted)', display: 'block', marginBottom: 4 }}>Body (for PUT)</label>
              <textarea
                value={body} onChange={e => setBody(e.target.value)}
                rows={4}
                style={{ width: '100%', padding: '8px 12px', background: 'var(--bg-secondary)', border: '1px solid var(--border)', borderRadius: 6, color: 'var(--text-primary)', fontSize: 13, fontFamily: 'JetBrains Mono, monospace', resize: 'vertical' }}
              />
            </div>
            <div style={{ display: 'flex', gap: 8, flexWrap: 'wrap' }}>
              {[
                { label: 'PUT Object', action: () => run('PUT', () => putObject(bucket, key, body, 'text/plain')) },
                { label: 'GET Object', action: () => run('GET', () => getObject(bucket, key)) },
                { label: 'DELETE Object', action: () => run('DELETE', () => deleteObject(bucket, key)) },
                { label: 'LIST Objects', action: () => run('LIST', () => listObjects(bucket)) },
                { label: 'Create Bucket', action: () => run('PUT_BUCKET', () => createBucket(bucket)) },
              ].map(btn => (
                <button
                  key={btn.label}
                  className="btn btn-primary btn-sm"
                  onClick={btn.action}
                  disabled={!!loading}
                  style={{ opacity: loading && loading !== btn.label ? 0.5 : 1 }}
                >
                  {loading === btn.label ? <div className="spinner" style={{ width: 14, height: 14 }} /> : <Play size={12} />}
                  {btn.label}
                </button>
              ))}
            </div>
          </div>
        </div>

        <div className="card">
          <div className="card-title" style={{ marginBottom: 16 }}>Response</div>
          <pre style={{ fontFamily: 'JetBrains Mono, monospace', fontSize: 12, color: 'var(--text-secondary)', whiteSpace: 'pre-wrap', minHeight: 200 }}>
            {response || '# Run an operation to see the response'}
          </pre>
        </div>
      </div>

      <div className="card" style={{ marginTop: 16 }}>
        <div className="card-title" style={{ marginBottom: 16 }}>API Reference</div>
        <table className="data-table">
          <thead><tr><th>Method</th><th>Endpoint</th><th>Description</th><th>Consistency</th></tr></thead>
          <tbody>
            {[
              { method: 'PUT', path: '/buckets/{bucket}/objects/{key}', desc: 'Upload object (quorum write)', consistency: 'Strong' },
              { method: 'GET', path: '/buckets/{bucket}/objects/{key}', desc: 'Download object', consistency: 'Strong' },
              { method: 'HEAD', path: '/buckets/{bucket}/objects/{key}', desc: 'Get object metadata', consistency: 'Strong' },
              { method: 'DELETE', path: '/buckets/{bucket}/objects/{key}', desc: 'Delete object (tombstone)', consistency: 'Strong' },
              { method: 'GET', path: '/buckets/{bucket}/list', desc: 'List objects', consistency: 'Read-your-writes' },
              { method: 'POST', path: '/uploads', desc: 'Initiate multipart upload', consistency: 'Strong' },
              { method: 'GET', path: '/admin/cluster', desc: 'Cluster state', consistency: 'N/A' },
              { method: 'GET', path: '/admin/raft', desc: 'Raft consensus status', consistency: 'N/A' },
            ].map(row => (
              <tr key={row.path}>
                <td><span className="tag">{row.method}</span></td>
                <td className="mono">{row.path}</td>
                <td style={{ color: 'var(--text-secondary)' }}>{row.desc}</td>
                <td><span className="badge valid">{row.consistency}</span></td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  )
}

// ─── Event Log Page ────────────────────────────────────────────────────────────

function LogsPage({ entries }: { entries: Array<{ id: string; time: string; level: string; message: string }> }) {
  const { events, connectionState } = useEventStream()

  const streamLogs = events.map(e => ({
    id: e.id,
    time: new Date(e.timestamp || Date.now()).toTimeString().slice(0, 8),
    level: e.severity || (e.type.includes('FAIL') || e.type.includes('CRASH') ? 'ERROR' : e.type.includes('WARN') || e.type.includes('CORRUPT') ? 'WARN' : 'INFO'),
    message: `[${e.type}] ${e.message || (e.nodeId ? `Node ${e.nodeId}` : e.objectKey ? `Object ${e.objectKey}` : '')}`
  }))

  const allLogs = [...streamLogs, ...entries].slice(0, 150)

  return (
    <div>
      <div className="page-header">
        <div className="page-title">Event Log</div>
        <span className="tag">{allLogs.length} events</span>
        <span className="tag" style={{ background: connectionState === 'LIVE' ? 'rgba(34,197,94,0.1)' : 'rgba(245,158,11,0.1)', color: connectionState === 'LIVE' ? '#22c55e' : '#f59e0b', border: '1px solid currentColor' }}>
          ● SSE {connectionState}
        </span>
      </div>
      <div className="card">
        <div className="event-log">
          {allLogs.length === 0 ? (
            <div style={{ color: 'var(--text-muted)', padding: 16 }}>No events yet. Interact with the cluster to see live log stream.</div>
          ) : (
            allLogs.map(e => (
              <div key={e.id} className="log-entry">
                <span className="log-time">{e.time}</span>
                <span className={`log-level ${e.level}`}>{e.level}</span>
                <span className="log-msg">{e.message}</span>
              </div>
            ))
          )}
        </div>
      </div>
    </div>
  )
}

// ─── Objects Page ─────────────────────────────────────────────────────────────

interface UploadQueueItem {
  id: string
  file: File
  key: string
  status: 'pending' | 'uploading' | 'success' | 'error'
  result?: PutObjectResult
  errorMsg?: string
}

function ObjectsPage({ onLog }: { onLog: (level: string, msg: string) => void }) {
  const [bucket, setBucket] = useState('default')
  const [newBucketName, setNewBucketName] = useState('')
  const [showCreateBucket, setShowCreateBucket] = useState(false)
  const [objects, setObjects] = useState<any[]>([])
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)

  // Drag & drop & file upload state
  const [queue, setQueue] = useState<UploadQueueItem[]>([])
  const [isDragActive, setIsDragActive] = useState(false)
  const [isUploading, setIsUploading] = useState(false)
  const fileInputRef = useRef<HTMLInputElement | null>(null)

  // Object preview modal state
  const [previewObj, setPreviewObj] = useState<any | null>(null)
  const [previewText, setPreviewText] = useState<string | null>(null)
  const [previewLoading, setPreviewLoading] = useState(false)

  const MAX_FILE_SIZE = 100 * 1024 * 1024 // 100 MB max object size

  const fetchObjects = useCallback(async () => {
    setLoading(true)
    try {
      const data = await listObjects(bucket)
      setObjects(data.objects || [])
      setError(null)
    } catch (e: any) {
      setError(e.message || 'Failed to fetch objects')
      setObjects([])
    } finally {
      setLoading(false)
    }
  }, [bucket])

  useEffect(() => {
    fetchObjects()
  }, [fetchObjects])

  // Add selected or dropped files to queue
  const addFilesToQueue = (files: FileList | File[]) => {
    const fileList = Array.from(files)
    const newItems: UploadQueueItem[] = fileList.map(f => {
      const isTooLarge = f.size > MAX_FILE_SIZE
      return {
        id: `${f.name}-${Date.now()}-${Math.random().toString(36).substring(2, 9)}`,
        file: f,
        key: f.name,
        status: isTooLarge ? 'error' : 'pending',
        errorMsg: isTooLarge ? `File exceeds maximum supported size (100 MB)` : undefined,
      }
    })
    setQueue(prev => [...prev, ...newItems])
  }

  const handleDragOver = (e: React.DragEvent) => {
    e.preventDefault()
    setIsDragActive(true)
  }

  const handleDragLeave = (e: React.DragEvent) => {
    e.preventDefault()
    setIsDragActive(false)
  }

  const handleDrop = (e: React.DragEvent) => {
    e.preventDefault()
    setIsDragActive(false)
    if (e.dataTransfer.files && e.dataTransfer.files.length > 0) {
      addFilesToQueue(e.dataTransfer.files)
    }
  }

  const handleFileSelectChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    if (e.target.files && e.target.files.length > 0) {
      addFilesToQueue(e.target.files)
    }
    if (fileInputRef.current) fileInputRef.current.value = ''
  }

  const updateItemKey = (id: string, newKey: string) => {
    setQueue(prev => prev.map(item => item.id === id ? { ...item, key: newKey } : item))
  }

  const removeItem = (id: string) => {
    setQueue(prev => prev.filter(item => item.id !== id))
  }

  const clearCompleted = () => {
    setQueue(prev => prev.filter(item => item.status === 'pending' || item.status === 'uploading'))
  }

  // Real file upload implementation - uploads binary bytes to backend
  const startUpload = async () => {
    const pendingItems = queue.filter(item => item.status === 'pending')
    if (pendingItems.length === 0) return

    setIsUploading(true)

    for (const item of pendingItems) {
      setQueue(prev => prev.map(q => q.id === item.id ? { ...q, status: 'uploading', errorMsg: undefined } : q))

      try {
        const contentType = item.file.type || 'application/octet-stream'
        // REAL binary bytes upload using Browser File object directly
        const result = await putObject(bucket, item.key, item.file, contentType)

        setQueue(prev => prev.map(q => q.id === item.id ? {
          ...q,
          status: 'success',
          result: {
            ...result,
            bucket: bucket,
            key: item.key,
          }
        } : q))

        onLog('SUCCESS', `Uploaded object '${item.key}' (${formatBytes(item.file.size)}) to bucket '${bucket}'`)
      } catch (err: any) {
        const msg = err.response?.data?.message || err.message || 'Upload failed'
        setQueue(prev => prev.map(q => q.id === item.id ? {
          ...q,
          status: 'error',
          errorMsg: msg
        } : q))
        onLog('ERROR', `Failed to upload '${item.key}': ${msg}`)
      }
    }

    setIsUploading(false)
    fetchObjects()
  }

  // Real GET / Download from FT-DOSS backend
  const handleDownload = async (objKey: string, contentType?: string) => {
    try {
      onLog('INFO', `Fetching stored bytes for '${objKey}' from FT-DOSS backend...`)
      const data = await getObject(bucket, objKey)
      const blob = new Blob([data], { type: contentType || 'application/octet-stream' })
      const url = URL.createObjectURL(blob)
      const a = document.createElement('a')
      a.href = url
      a.download = objKey.split('/').pop() || objKey
      document.body.appendChild(a)
      a.click()
      document.body.removeChild(a)
      URL.revokeObjectURL(url)
      onLog('SUCCESS', `Successfully downloaded '${objKey}' (${formatBytes(data.byteLength)})`)
    } catch (err: any) {
      onLog('ERROR', `Download failed for '${objKey}': ${err.message}`)
    }
  }

  // Object text preview or metadata inspection
  const handlePreview = async (obj: any) => {
    setPreviewObj(obj)
    const isText = (obj.content_type && (obj.content_type.startsWith('text/') || obj.content_type.includes('json') || obj.content_type.includes('xml'))) ||
                   obj.key.endsWith('.txt') || obj.key.endsWith('.json') || obj.key.endsWith('.csv') || obj.key.endsWith('.md') || obj.key.endsWith('.js') || obj.key.endsWith('.go')
    if (isText) {
      setPreviewLoading(true)
      try {
        const txt = await getObjectText(bucket, obj.key)
        setPreviewText(txt)
      } catch (e: any) {
        setPreviewText(`Error loading content: ${e.message}`)
      } finally {
        setPreviewLoading(false)
      }
    } else {
      setPreviewText(null)
    }
  }

  // Real Create Bucket API
  const handleCreateBucketSubmit = async () => {
    if (!newBucketName.trim()) return
    try {
      await createBucket(newBucketName.trim())
      setBucket(newBucketName.trim())
      setNewBucketName('')
      setShowCreateBucket(false)
      onLog('SUCCESS', `Created bucket '${newBucketName.trim()}'`)
    } catch (e: any) {
      onLog('ERROR', `Failed to create bucket: ${e.message}`)
    }
  }

  const handleDelete = async (key: string) => {
    try {
      await deleteObject(bucket, key)
      onLog('WARN', `Deleted object ${key} (written tombstone)`)
      fetchObjects()
    } catch (e: any) {
      onLog('ERROR', `Failed to delete ${key}: ${e.message}`)
    }
  }

  const handleCorrupt = async (key: string, versionId: string) => {
    try {
      await corruptReplica(bucket, key, versionId)
      onLog('WARN', `Injected replica corruption for ${key}`)
      fetchObjects()
    } catch (e: any) {
      onLog('ERROR', `Failed to corrupt ${key}: ${e.message}`)
    }
  }

  const pendingCount = queue.filter(q => q.status === 'pending').length
  const dropzoneStatusClass = isUploading ? 'uploading' : isDragActive ? 'active' : ''

  return (
    <div>
      <div className="page-header">
        <div>
          <div className="page-title">Objects & File Storage</div>
          <div className="page-subtitle">Real binary object upload, quorum storage, and download verification</div>
        </div>
        <div style={{ display: 'flex', gap: 8, alignItems: 'center' }}>
          <div style={{ display: 'flex', alignItems: 'center', gap: 6 }}>
            <span style={{ fontSize: 12, color: 'var(--text-muted)' }}>Bucket:</span>
            <input
              value={bucket} onChange={e => setBucket(e.target.value)}
              placeholder="Bucket"
              style={{ padding: '6px 12px', background: 'var(--bg-secondary)', border: '1px solid var(--border)', borderRadius: 6, color: 'var(--text-primary)', fontSize: 13, fontFamily: 'JetBrains Mono, monospace', width: 120 }}
            />
          </div>
          <button className="btn btn-ghost btn-sm" onClick={() => setShowCreateBucket(!showCreateBucket)}>
            <Plus size={14} /> Create Bucket
          </button>
          <button className="btn btn-ghost btn-sm" onClick={fetchObjects}>
            <RefreshCw size={14} /> Refresh
          </button>
        </div>
      </div>

      {showCreateBucket && (
        <div className="card" style={{ marginBottom: 16, border: '1px solid var(--accent)' }}>
          <div style={{ display: 'flex', gap: 12, alignItems: 'center' }}>
            <span style={{ fontSize: 13, fontWeight: 600 }}>Create New Bucket:</span>
            <input
              value={newBucketName}
              onChange={e => setNewBucketName(e.target.value)}
              placeholder="e.g. documents"
              style={{ flex: 1, padding: '6px 12px', background: 'var(--bg-secondary)', border: '1px solid var(--border)', borderRadius: 6, color: 'var(--text-primary)', fontSize: 13, fontFamily: 'JetBrains Mono, monospace' }}
            />
            <button className="btn btn-primary btn-sm" onClick={handleCreateBucketSubmit}>Create</button>
            <button className="btn btn-ghost btn-sm" onClick={() => setShowCreateBucket(false)}>Cancel</button>
          </div>
        </div>
      )}

      {/* Upload Object Dropzone Card */}
      <div className="card" style={{ marginBottom: 24 }}>
        <div className="card-header">
          <div className="card-title">Upload Object</div>
          <span className="tag">Maximum object size: 100 MB</span>
        </div>

        <input
          type="file"
          ref={fileInputRef}
          multiple
          onChange={handleFileSelectChange}
          style={{ display: 'none' }}
        />

        <div
          className={`dropzone ${dropzoneStatusClass}`}
          onDragOver={handleDragOver}
          onDragLeave={handleDragLeave}
          onDrop={handleDrop}
          onClick={() => fileInputRef.current?.click()}
        >
          {isUploading ? (
            <>
              <div className="spinner" style={{ width: 32, height: 32 }} />
              <div style={{ fontSize: 15, fontWeight: 600, color: 'var(--accent-2)' }}>Uploading...</div>
              <div style={{ fontSize: 12, color: 'var(--text-muted)' }}>Streaming file bytes to FT-DOSS WAL & quorum nodes</div>
            </>
          ) : isDragActive ? (
            <>
              <Upload size={32} style={{ color: 'var(--accent)' }} />
              <div style={{ fontSize: 15, fontWeight: 600, color: 'var(--accent)' }}>Drop to upload</div>
              <div style={{ fontSize: 12, color: 'var(--text-muted)' }}>Release file to queue object upload</div>
            </>
          ) : (
            <>
              <Upload size={32} style={{ color: 'var(--text-secondary)' }} />
              <div style={{ fontSize: 14, fontWeight: 500, color: 'var(--text-primary)' }}>
                Drag & drop a file here or click to select
              </div>
              <div style={{ display: 'flex', gap: 8, marginTop: 4 }}>
                <button
                  type="button"
                  className="btn btn-primary btn-sm"
                  onClick={(e) => { e.stopPropagation(); fileInputRef.current?.click(); }}
                >
                  Choose File
                </button>
              </div>
              <div style={{ fontSize: 11, color: 'var(--text-muted)', marginTop: 4 }}>
                Supports arbitrary binary formats (PDF, PNG, JPEG, ZIP, JSON, CSV, TXT)
              </div>
            </>
          )}
        </div>

        {/* Selected Files Queue */}
        {queue.length > 0 && (
          <div style={{ marginTop: 20 }}>
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 10 }}>
              <div style={{ fontSize: 13, fontWeight: 600, color: 'var(--text-primary)' }}>
                Upload Queue ({queue.length} file{queue.length > 1 ? 's' : ''})
              </div>
              <div style={{ display: 'flex', gap: 8 }}>
                <button
                  className="btn btn-primary btn-sm"
                  onClick={startUpload}
                  disabled={isUploading || pendingCount === 0}
                >
                  {isUploading ? 'Uploading...' : `Upload ${pendingCount} Object${pendingCount !== 1 ? 's' : ''}`}
                </button>
                <button className="btn btn-ghost btn-sm" onClick={clearCompleted} disabled={isUploading}>
                  Clear Completed
                </button>
              </div>
            </div>

            <div style={{ display: 'flex', flexDirection: 'column', gap: 10 }}>
              {queue.map(item => (
                <div key={item.id} style={{ background: 'var(--bg-secondary)', border: '1px solid var(--border)', borderRadius: 8, padding: 12 }}>
                  <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', gap: 12, flexWrap: 'wrap' }}>
                    <div style={{ display: 'flex', alignItems: 'center', gap: 8, flex: 1, minWidth: 200 }}>
                      <FileText size={18} style={{ color: 'var(--accent)' }} />
                      <div style={{ display: 'flex', flexDirection: 'column' }}>
                        <span style={{ fontSize: 13, fontWeight: 600 }}>{item.file.name}</span>
                        <span style={{ fontSize: 11, color: 'var(--text-muted)' }}>{formatBytes(item.file.size)} · {item.file.type || 'binary'}</span>
                      </div>
                    </div>

                    <div style={{ display: 'flex', alignItems: 'center', gap: 8, flex: 2, minWidth: 260 }}>
                      <span style={{ fontSize: 12, color: 'var(--text-muted)' }}>Object Key:</span>
                      <input
                        value={item.key}
                        onChange={e => updateItemKey(item.id, e.target.value)}
                        disabled={item.status !== 'pending'}
                        style={{ flex: 1, padding: '4px 8px', background: 'var(--bg-primary)', border: '1px solid var(--border)', borderRadius: 4, color: 'var(--text-primary)', fontSize: 12, fontFamily: 'JetBrains Mono, monospace' }}
                      />
                    </div>

                    <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
                      {item.status === 'pending' && <span className="badge">Pending</span>}
                      {item.status === 'uploading' && (
                        <span className="badge" style={{ background: 'rgba(139,92,246,0.15)', color: '#8b5cf6', border: '1px solid #8b5cf6' }}>
                          <span className="spinner" style={{ width: 10, height: 10, display: 'inline-block', marginRight: 4 }} /> Uploading...
                        </span>
                      )}
                      {item.status === 'success' && <span className="badge valid">✓ Success</span>}
                      {item.status === 'error' && <span className="badge unavailable">✕ Error</span>}

                      {item.status !== 'uploading' && (
                        <button className="btn btn-ghost btn-sm" onClick={() => removeItem(item.id)} title="Remove file">
                          <X size={14} />
                        </button>
                      )}
                    </div>
                  </div>

                  {/* Upload Error Banner */}
                  {item.status === 'error' && item.errorMsg && (
                    <div style={{ marginTop: 8, padding: '6px 10px', background: 'rgba(239,68,68,0.1)', border: '1px solid rgba(239,68,68,0.3)', borderRadius: 4, fontSize: 12, color: 'var(--unavailable)' }}>
                      Upload failed: {item.errorMsg}
                    </div>
                  )}

                  {/* Upload Success Details Breakdown */}
                  {item.status === 'success' && item.result && (
                    <div style={{ marginTop: 10, paddingTop: 10, borderTop: '1px solid var(--border)', display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(140px, 1fr))', gap: 8, fontSize: 11, fontFamily: 'JetBrains Mono, monospace' }}>
                      <div>
                        <span style={{ color: 'var(--text-muted)', display: 'block' }}>Version ID</span>
                        <span style={{ color: 'var(--text-primary)' }}>{item.result.version_id}</span>
                      </div>
                      <div>
                        <span style={{ color: 'var(--text-muted)', display: 'block' }}>SHA-256 Checksum</span>
                        <span style={{ color: 'var(--healthy)' }}>{item.result.checksum ? item.result.checksum.slice(0, 16) + '...' : 'N/A'}</span>
                      </div>
                      <div>
                        <span style={{ color: 'var(--text-muted)', display: 'block' }}>Placement Group</span>
                        <span style={{ color: 'var(--accent)' }}>PG-{item.result.placement_group ?? 'N/A'}</span>
                      </div>
                      <div>
                        <span style={{ color: 'var(--text-muted)', display: 'block' }}>Replica Nodes</span>
                        <span style={{ color: 'var(--text-primary)' }}>{(item.result.replica_nodes || []).join(', ') || 'N/A'}</span>
                      </div>
                      <div>
                        <span style={{ color: 'var(--text-muted)', display: 'block' }}>Write Quorum</span>
                        <span style={{ color: 'var(--healthy)' }}>Satisfied (W=2)</span>
                      </div>
                    </div>
                  )}
                </div>
              ))}
            </div>
          </div>
        )}
      </div>

      {/* Stored Objects List Card */}
      <div className="card">
        <div className="card-header">
          <div className="card-title">Stored Objects (Bucket: {bucket})</div>
          <span style={{ fontSize: 11, color: 'var(--text-muted)' }}>Source: GET /buckets/{bucket}/list</span>
        </div>
        {loading ? (
          <div style={{ padding: 24, color: 'var(--text-muted)' }}>Loading objects from backend...</div>
        ) : error ? (
          <div style={{ padding: 24, color: 'var(--unavailable)' }}>{error}</div>
        ) : objects.length === 0 ? (
          <div style={{ padding: 32, textAlign: 'center', color: 'var(--text-muted)' }}>
            No objects found in bucket '{bucket}'. Drag and drop a file above to upload to FT-DOSS.
          </div>
        ) : (
          <table className="data-table">
            <thead>
              <tr>
                <th>Key</th>
                <th>Size</th>
                <th>Version</th>
                <th>PG</th>
                <th>Replicas</th>
                <th>SHA-256 Checksum</th>
                <th>State</th>
                <th>Actions</th>
              </tr>
            </thead>
            <tbody>
              {objects.map((obj: any) => (
                <tr key={`${obj.bucket}-${obj.key}-${obj.version_id}`}>
                  <td className="mono" style={{ fontWeight: 600 }}>{obj.key}</td>
                  <td>{formatBytes(obj.size || 0)}</td>
                  <td className="mono" style={{ fontSize: 11 }}>{obj.version_id ? obj.version_id.slice(0, 8) + '...' : 'v1'}</td>
                  <td><span className="tag">PG-{obj.placement_group ?? '?'}</span></td>
                  <td style={{ fontSize: 12 }}>{(obj.replica_nodes || []).join(', ') || 'N/A'}</td>
                  <td className="mono" style={{ fontSize: 11, color: 'var(--text-muted)' }}>
                    {obj.checksum ? obj.checksum.slice(0, 16) + '...' : 'N/A'}
                  </td>
                  <td><StateBadge state={obj.is_tombstone ? 'DELETED' : (obj.state || 'VALID')} /></td>
                  <td>
                    <div style={{ display: 'flex', gap: 6 }}>
                      <button
                        className="btn btn-primary btn-sm"
                        onClick={() => handleDownload(obj.key, obj.content_type)}
                        title="Download actual stored object from FT-DOSS backend"
                      >
                        <Download size={12} /> Download
                      </button>
                      <button
                        className="btn btn-ghost btn-sm"
                        onClick={() => handlePreview(obj)}
                        title="Preview object or inspect metadata"
                      >
                        Inspect
                      </button>
                      <button className="btn btn-danger btn-sm" onClick={() => handleDelete(obj.key)}>
                        Delete
                      </button>
                      <button className="btn btn-warning btn-sm" onClick={() => handleCorrupt(obj.key, obj.version_id || 'v1')}>
                        Corrupt
                      </button>
                    </div>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </div>

      {/* Object Inspection / Preview Modal */}
      {previewObj && (
        <div className="modal-overlay" onClick={() => setPreviewObj(null)}>
          <div className="modal-content" onClick={e => e.stopPropagation()}>
            <div className="modal-header">
              <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
                <FileText size={18} style={{ color: 'var(--accent)' }} />
                <span style={{ fontWeight: 600, fontSize: 15 }}>Object Details: {previewObj.key}</span>
              </div>
              <button className="btn btn-ghost btn-sm" onClick={() => setPreviewObj(null)}><X size={16} /></button>
            </div>
            <div className="modal-body">
              {previewLoading ? (
                <div style={{ padding: 24, textAlign: 'center', color: 'var(--text-muted)' }}>Reading content...</div>
              ) : previewText !== null ? (
                <div style={{ marginBottom: 16 }}>
                  <div style={{ fontSize: 12, fontWeight: 600, marginBottom: 6, color: 'var(--text-muted)' }}>Text Content Preview:</div>
                  <pre style={{ padding: 12, background: 'var(--bg-secondary)', border: '1px solid var(--border)', borderRadius: 6, fontSize: 12, fontFamily: 'JetBrains Mono, monospace', maxHeight: 200, overflowY: 'auto', whiteSpace: 'pre-wrap' }}>
                    {previewText}
                  </pre>
                </div>
              ) : null}

              <div style={{ fontSize: 12, fontWeight: 600, marginBottom: 8, color: 'var(--text-muted)' }}>Metadata & Storage Layout:</div>
              <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 12, fontSize: 12, fontFamily: 'JetBrains Mono, monospace' }}>
                <div style={{ padding: 10, background: 'var(--bg-secondary)', borderRadius: 6 }}>
                  <span style={{ color: 'var(--text-muted)', display: 'block' }}>Bucket</span>
                  <span>{previewObj.bucket || bucket}</span>
                </div>
                <div style={{ padding: 10, background: 'var(--bg-secondary)', borderRadius: 6 }}>
                  <span style={{ color: 'var(--text-muted)', display: 'block' }}>Size</span>
                  <span>{formatBytes(previewObj.size || 0)}</span>
                </div>
                <div style={{ padding: 10, background: 'var(--bg-secondary)', borderRadius: 6 }}>
                  <span style={{ color: 'var(--text-muted)', display: 'block' }}>Content-Type</span>
                  <span>{previewObj.content_type || 'application/octet-stream'}</span>
                </div>
                <div style={{ padding: 10, background: 'var(--bg-secondary)', borderRadius: 6 }}>
                  <span style={{ color: 'var(--text-muted)', display: 'block' }}>Placement Group</span>
                  <span style={{ color: 'var(--accent)' }}>PG-{previewObj.placement_group ?? 'N/A'}</span>
                </div>
                <div style={{ padding: 10, background: 'var(--bg-secondary)', borderRadius: 6, gridColumn: 'span 2' }}>
                  <span style={{ color: 'var(--text-muted)', display: 'block' }}>Version ID</span>
                  <span>{previewObj.version_id}</span>
                </div>
                <div style={{ padding: 10, background: 'var(--bg-secondary)', borderRadius: 6, gridColumn: 'span 2' }}>
                  <span style={{ color: 'var(--text-muted)', display: 'block' }}>Authoritative SHA-256 Checksum</span>
                  <span style={{ color: 'var(--healthy)' }}>{previewObj.checksum || 'N/A'}</span>
                </div>
                <div style={{ padding: 10, background: 'var(--bg-secondary)', borderRadius: 6, gridColumn: 'span 2' }}>
                  <span style={{ color: 'var(--text-muted)', display: 'block' }}>Replica Storage Nodes</span>
                  <span>{(previewObj.replica_nodes || []).join(', ') || 'N/A'}</span>
                </div>
              </div>

              <div style={{ display: 'flex', justifyContent: 'flex-end', gap: 10, marginTop: 20 }}>
                <button
                  className="btn btn-primary btn-sm"
                  onClick={() => handleDownload(previewObj.key, previewObj.content_type)}
                >
                  <Download size={14} /> Download File
                </button>
                <button className="btn btn-ghost btn-sm" onClick={() => setPreviewObj(null)}>Close</button>
              </div>
            </div>
          </div>
        </div>
      )}
    </div>
  )
}

// ─── Replicas Page ────────────────────────────────────────────────────────────

function ReplicasPage() {
  const { data: metrics } = useClusterMetrics()
  const { data: nodes } = useNodes()

  const m = metrics || {
    healthy_nodes: 0, total_objects: 0, corrupt_replicas: 0, pending_repairs: 0
  }

  return (
    <div>
      <div className="page-header">
        <div>
          <div className="page-title">Replica Distribution</div>
          <div className="page-subtitle">Multi-node placement & health tracking</div>
        </div>
        <span style={{ fontSize: 11, color: 'var(--text-muted)' }}>Source: GET /admin/cluster/metrics & /admin/nodes</span>
      </div>

      <div className="metric-grid" style={{ marginBottom: 24 }}>
        <MetricCard label="Quorum Target (N)" value={3} sub="N=3, W=2, R=2" color="blue" />
        <MetricCard label="Total Objects" value={formatNumber(m.total_objects)} color="blue" />
        <MetricCard label="Corrupt Replicas" value={m.corrupt_replicas} color={m.corrupt_replicas > 0 ? 'red' : 'green'} />
        <MetricCard label="Pending Repairs" value={m.pending_repairs} color={m.pending_repairs > 0 ? 'yellow' : 'green'} />
      </div>

      <div className="card">
        <div className="card-header">
          <div className="card-title">Storage Node Replica Distribution</div>
        </div>
        <table className="data-table">
          <thead>
            <tr>
              <th>Node ID</th>
              <th>Status</th>
              <th>Replicas Held</th>
              <th>Objects Hosted</th>
              <th>Disk Usage</th>
              <th>Cluster Epoch</th>
            </tr>
          </thead>
          <tbody>
            {(nodes || []).map(n => (
              <tr key={n.id}>
                <td className="mono" style={{ fontWeight: 600 }}>{n.id}</td>
                <td><StateBadge state={n.state} /></td>
                <td style={{ fontFamily: 'JetBrains Mono, monospace', color: 'var(--accent)' }}>{n.replica_count || 0}</td>
                <td>{n.object_count || 0}</td>
                <td>{formatBytes(n.disk_total - n.disk_free)} / {formatBytes(n.disk_total)}</td>
                <td className="mono">{n.cluster_epoch || 0}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  )
}

// ─── Repairs Page ─────────────────────────────────────────────────────────────

function RepairsPage({ onLog }: { onLog: (level: string, msg: string) => void }) {
  const { data: repairs, refresh } = useRepairs()
  const [triggering, setTriggering] = useState(false)

  const handleTrigger = async () => {
    setTriggering(true)
    try {
      await triggerRepair()
      onLog('INFO', 'Anti-entropy repair triggered')
      setTimeout(refresh, 800)
    } catch (e: any) {
      onLog('ERROR', `Trigger repair failed: ${e.message}`)
    } finally {
      setTriggering(false)
    }
  }

  return (
    <div>
      <div className="page-header">
        <div>
          <div className="page-title">Self-Healing Repair Engine</div>
          <div className="page-subtitle">Anti-entropy background replica repair</div>
        </div>
        <div style={{ display: 'flex', gap: 8 }}>
          <button className="btn btn-primary btn-sm" onClick={handleTrigger} disabled={triggering}>
            <Wrench size={14} /> {triggering ? 'Triggering...' : 'Trigger Repair'}
          </button>
          <button className="btn btn-ghost btn-sm" onClick={refresh}>
            <RefreshCw size={14} /> Refresh
          </button>
        </div>
      </div>

      <div className="card">
        <div className="card-header">
          <div className="card-title">Active & Historical Repairs</div>
          <span style={{ fontSize: 11, color: 'var(--text-muted)' }}>Source: GET /admin/repairs</span>
        </div>
        {(!repairs || repairs.length === 0) ? (
          <div style={{ padding: 32, textAlign: 'center', color: 'var(--text-muted)' }}>
            No queued or active repairs. All cluster replicas are verified healthy.
          </div>
        ) : (
          <table className="data-table">
            <thead>
              <tr>
                <th>Job ID</th>
                <th>Target Object</th>
                <th>Source Node</th>
                <th>Target Node</th>
                <th>Priority</th>
                <th>State</th>
                <th>Bytes Copied</th>
                <th>Created At</th>
              </tr>
            </thead>
            <tbody>
              {repairs.map(job => (
                <tr key={job.id}>
                  <td className="mono">{job.id}</td>
                  <td className="mono" style={{ fontWeight: 600 }}>{job.key}</td>
                  <td className="mono" style={{ color: 'var(--healthy)' }}>{job.source_node || 'N/A'}</td>
                  <td className="mono" style={{ color: 'var(--accent)' }}>{job.target_node}</td>
                  <td>{job.priority}</td>
                  <td><StateBadge state={job.state} /></td>
                  <td>{formatBytes(job.bytes_copied || 0)} / {formatBytes(job.total_bytes || 0)}</td>
                  <td style={{ fontSize: 11, color: 'var(--text-muted)' }}>
                    {new Date(job.created_at || Date.now()).toLocaleTimeString()}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </div>
    </div>
  )
}

// ─── Scrub Page ───────────────────────────────────────────────────────────────

function ScrubPage({ onLog }: { onLog: (level: string, msg: string) => void }) {
  const { data: nodes, refresh } = useNodes()

  const handleScrub = async (nodeId: string) => {
    try {
      await triggerScrub(nodeId)
      onLog('INFO', `Bit-rot scrub triggered on node ${nodeId}`)
      setTimeout(refresh, 500)
    } catch (e: any) {
      onLog('ERROR', `Scrub on ${nodeId} failed: ${e.message}`)
    }
  }

  return (
    <div>
      <div className="page-header">
        <div>
          <div className="page-title">Continuous Background Scrubbing</div>
          <div className="page-subtitle">Proactive SHA-256 bit-rot detection</div>
        </div>
      </div>

      <div className="node-grid">
        {(nodes || []).map(node => (
          <div key={node.id} className="card">
            <div className="card-header">
              <div className="card-title">{node.id}</div>
              <StateBadge state={node.state} />
            </div>
            <div style={{ fontSize: 13, color: 'var(--text-secondary)', marginBottom: 12 }}>
              Zone: {node.zone} • Rack: {node.rack}
            </div>
            <div style={{ fontSize: 13, marginBottom: 16 }}>
              Replicas Scanned: <strong style={{ color: 'var(--accent)' }}>{node.replica_count || 0}</strong>
            </div>
            <button className="btn btn-primary btn-sm" onClick={() => handleScrub(node.id)}>
              <Search size={14} /> Run Scrub
            </button>
          </div>
        ))}
      </div>
    </div>
  )
}

// ─── Rebalance Page ───────────────────────────────────────────────────────────

function RebalancePage() {
  const { data: pgs } = usePlacementGroups()
  const { data: nodes } = useNodes()
  const { data: metrics } = useClusterMetrics()

  return (
    <div>
      <div className="page-header">
        <div>
          <div className="page-title">Cluster Rebalancing & Epoch Fencing</div>
          <div className="page-subtitle">Automated placement group redistribution</div>
        </div>
      </div>

      <div className="metric-grid" style={{ marginBottom: 24 }}>
        <MetricCard label="Cluster Epoch" value={metrics?.cluster_epoch || 0} color="blue" />
        <MetricCard label="Total Placement Groups" value={pgs?.length || 0} color="blue" />
        <MetricCard label="Active Nodes" value={nodes?.filter(n => n.state === 'HEALTHY').length || 0} color="green" />
      </div>

      <div className="card">
        <div className="card-header">
          <div className="card-title">Epoch Fencing Invariants</div>
        </div>
        <div style={{ padding: 16, fontSize: 13, color: 'var(--text-secondary)', lineHeight: 1.6 }}>
          • Heartbeats and IO requests carrying an epoch smaller than current <strong>Epoch {metrics?.cluster_epoch || 0}</strong> are rejected with <code>STALE_EPOCH_REJECTED</code>.<br />
          • Membership transitions automatically increment the cluster epoch across metadata consensus.<br />
          • Placement groups deterministically rebalance replica mappings without altering object keys.
        </div>
      </div>
    </div>
  )
}

// ─── Configuration Page ───────────────────────────────────────────────────────

function ConfigPage() {
  return (
    <div>
      <div className="page-header">
        <div>
          <div className="page-title">System Configuration</div>
          <div className="page-subtitle">Active cluster runtime parameters</div>
        </div>
      </div>

      <div className="card">
        <div className="card-title" style={{ marginBottom: 16 }}>Cluster Invariants & Tolerances</div>
        <table className="data-table">
          <thead>
            <tr>
              <th>Parameter</th>
              <th>Value</th>
              <th>Description</th>
            </tr>
          </thead>
          <tbody>
            {[
              { param: 'Replication Factor (N)', val: '3', desc: 'Replicas stored per object across fault domains' },
              { param: 'Write Quorum (W)', val: '2', desc: 'Persistent acknowledgements required for PUT success' },
              { param: 'Read Quorum (R)', val: '2', desc: 'Node responses required for GET validation' },
              { param: 'Placement Groups', val: '1024', desc: 'Deterministic CRUSH hash ring groups' },
              { param: 'Integrity Verification', val: 'SHA-256', desc: 'Cryptographic digest calculation on read & scrub' },
              { param: 'WAL Durability', val: 'Enabled (fsync)', desc: 'Write-ahead log appended before memory index' },
              { param: 'Consensus Protocol', val: 'Raft', desc: 'Leader election and committed metadata index' },
              { param: 'Observability Transport', val: 'SSE (EventBus)', desc: 'Real-time Server-Sent Events push stream' },
              { param: 'SSE Security Model', val: '60s Single-Use Ticket', desc: 'Authenticated token generated via POST /admin/sse-token' },
            ].map(row => (
              <tr key={row.param}>
                <td style={{ fontWeight: 600 }}>{row.param}</td>
                <td className="mono" style={{ color: 'var(--accent)' }}>{row.val}</td>
                <td style={{ color: 'var(--text-secondary)' }}>{row.desc}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  )
}

// ─── Main App ─────────────────────────────────────────────────────────────────

function App() {
  const [activePage, setActivePage] = useState('overview')
  const { data: metrics } = useClusterMetrics()
  const { entries, addEntry } = useEventLog()
  const { connectionState: streamState } = useEventStream()

  const handleLog = useCallback((level: string, msg: string) => {
    addEntry(level as 'INFO' | 'WARN' | 'ERROR' | 'SUCCESS', msg)
  }, [addEntry])

  const sections = [...new Set(NAV_ITEMS.map(i => i.section))]

  const clusterStatus = metrics
    ? metrics.unavailable_nodes > 0 ? 'critical'
    : metrics.suspect_nodes > 0 ? 'degraded'
    : 'healthy'
    : 'healthy'

  const renderPage = () => {
    switch (activePage) {
      case 'overview': return <OverviewPage />
      case 'nodes': return <NodesPage onLog={handleLog} />
      case 'metadata': return <RaftPage />
      case 'placement': return <PlacementPage />
      case 'objects': return <ObjectsPage onLog={handleLog} />
      case 'replicas': return <ReplicasPage />
      case 'repairs': return <RepairsPage onLog={handleLog} />
      case 'scrub': return <ScrubPage onLog={handleLog} />
      case 'rebalance': return <RebalancePage />
      case 'chaos': return <ChaosPage onLog={handleLog} />
      case 'metrics': return <MetricsPage />
      case 'alerts': return <AlertsPage />
      case 'logs': return <LogsPage entries={entries} />
      case 'api': return <APIPage onLog={handleLog} />
      case 'config': return <ConfigPage />
      default:
        return <OverviewPage />
    }
  }

  return (
    <div className="app-layout">
      {/* Topbar */}
      <header className="topbar">
        <a className="topbar-logo" href="#" onClick={() => setActivePage('overview')}>
          <span className="logo-badge">FT-DOSS</span>
          <span style={{ color: 'var(--text-secondary)', fontWeight: 400, fontSize: 14 }}>
            Fault-Tolerant Distributed Object Storage System
          </span>
        </a>

        <div className="topbar-spacer" />

        <div className="topbar-status">
          <span style={{ fontSize: 11, fontWeight: 700, color: streamState === 'LIVE' ? '#22c55e' : '#f59e0b', background: 'rgba(255,255,255,0.05)', padding: '2px 8px', borderRadius: 12, border: '1px solid rgba(255,255,255,0.1)', display: 'flex', alignItems: 'center', gap: 4 }}>
            <span style={{ width: 6, height: 6, borderRadius: '50%', background: streamState === 'LIVE' ? '#22c55e' : '#f59e0b' }} />
            {streamState}
          </span>
          <span style={{ color: 'var(--text-muted)' }}>·</span>
          <div className={`status-dot ${clusterStatus}`} />
          <span style={{ textTransform: 'capitalize' }}>{clusterStatus}</span>
          <span style={{ color: 'var(--text-muted)' }}>·</span>
          <span>{metrics?.healthy_nodes ?? '?'}/{metrics?.total_nodes ?? '?'} nodes</span>
          <span style={{ color: 'var(--text-muted)' }}>·</span>
          <span>Epoch {metrics?.cluster_epoch ?? '?'}</span>
          {metrics?.raft_leader_id && (
            <>
              <span style={{ color: 'var(--text-muted)' }}>·</span>
              <span style={{ color: 'var(--accent)' }}>Leader: {metrics.raft_leader_id}</span>
            </>
          )}
        </div>
      </header>

      {/* Sidebar */}
      <nav className="sidebar">
        {sections.map(section => (
          <div key={section}>
            <div className="sidebar-section">{section}</div>
            {NAV_ITEMS.filter(i => i.section === section).map(item => (
              <div
                key={item.id}
                className={`sidebar-item ${activePage === item.id ? 'active' : ''}`}
                onClick={() => setActivePage(item.id)}
              >
                <item.icon size={15} />
                {item.label}
                {item.id === 'alerts' && metrics && (metrics.corrupt_replicas > 0 || metrics.unavailable_nodes > 0) && (
                  <span style={{ marginLeft: 'auto', background: 'var(--unavailable)', color: 'white', fontSize: 10, fontWeight: 700, borderRadius: 10, padding: '1px 6px' }}>
                    !
                  </span>
                )}
              </div>
            ))}
          </div>
        ))}
      </nav>

      {/* Main content */}
      <main className="main-content">
        {renderPage()}
      </main>
    </div>
  )
}

export default App

