import { useState, useEffect, useCallback, useRef } from 'react'
import type { ClusterMetrics, StorageNode, RaftStatus, PlacementGroup, RepairJob, Alert } from '../services/api'
import {
  getClusterMetrics, getNodes, getRaftStatus,
  getPlacementGroups, getRepairs, getAlerts,
} from '../services/api'

// ─── useAutoRefresh: polling hook ───────────────────────────────────────────

export function useAutoRefresh<T>(
  fetcher: () => Promise<T>,
  intervalMs = 3000,
  enabled = true
): { data: T | null; loading: boolean; error: string | null; refresh: () => void } {
  const [data, setData] = useState<T | null>(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const fetcherRef = useRef(fetcher)
  fetcherRef.current = fetcher

  const refresh = useCallback(async () => {
    try {
      const result = await fetcherRef.current()
      setData(result)
      setError(null)
    } catch (e: unknown) {
      setError(e instanceof Error ? e.message : 'Fetch failed')
    } finally {
      setLoading(false)
    }
  }, [])

  useEffect(() => {
    if (!enabled) return
    refresh()
    const id = setInterval(refresh, intervalMs)
    return () => clearInterval(id)
  }, [refresh, intervalMs, enabled])

  return { data, loading, error, refresh }
}

// ─── Specific data hooks ───────────────────────────────────────────────────

export function useClusterMetrics(interval = 3000) {
  return useAutoRefresh<ClusterMetrics>(getClusterMetrics, interval)
}

export function useNodes(interval = 3000) {
  return useAutoRefresh<StorageNode[]>(getNodes, interval)
}

export function useRaftStatus(interval = 2000) {
  return useAutoRefresh<RaftStatus>(getRaftStatus, interval)
}

export function usePlacementGroups(interval = 5000) {
  return useAutoRefresh<PlacementGroup[]>(getPlacementGroups, interval)
}

export function useRepairs(interval = 3000) {
  return useAutoRefresh<RepairJob[]>(getRepairs, interval)
}

export function useAlerts(interval = 5000) {
  return useAutoRefresh<Alert[]>(getAlerts, interval)
}

// ─── useEventLog: captures system events ────────────────────────────────────

export interface LogEntry {
  id: string
  time: string
  level: 'INFO' | 'WARN' | 'ERROR' | 'SUCCESS'
  message: string
}

let logCounter = 0

export function useEventLog(maxEntries = 100) {
  const [entries, setEntries] = useState<LogEntry[]>([])

  const addEntry = useCallback((level: LogEntry['level'], message: string) => {
    const entry: LogEntry = {
      id: `log-${++logCounter}`,
      time: new Date().toTimeString().slice(0, 8),
      level,
      message,
    }
    setEntries(prev => [entry, ...prev].slice(0, maxEntries))
  }, [maxEntries])

  return { entries, addEntry }
}

// ─── useMetricsHistory: rolling metrics for charts ──────────────────────────

export function useMetricsHistory(maxPoints = 30) {
  const [history, setHistory] = useState<Array<{
    time: string
    reads: number
    writes: number
    latency_p50: number
    latency_p99: number
    healthy_nodes: number
    pending_repairs: number
  }>>([])

  const push = useCallback((metrics: ClusterMetrics) => {
    const point = {
      time: new Date().toTimeString().slice(0, 5),
      reads: metrics.successful_reads,
      writes: metrics.successful_writes,
      latency_p50: metrics.read_latency_p50_ms,
      latency_p99: metrics.read_latency_p99_ms,
      healthy_nodes: metrics.healthy_nodes,
      pending_repairs: metrics.pending_repairs,
    }
    setHistory(prev => [...prev, point].slice(-maxPoints))
  }, [maxPoints])

  return { history, push }
}
