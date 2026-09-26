import { useState, useEffect, useRef } from 'react'
import { getSSETicket } from '../services/api'

export interface StreamEvent {
  id: string
  type: string
  timestamp: string
  severity?: 'INFO' | 'WARN' | 'ERROR' | 'SUCCESS'
  source?: string
  nodeId?: string
  objectKey?: string
  message?: string
  data?: Record<string, unknown>
}

export type ConnectionState = 'LIVE' | 'RECONNECTING' | 'OFFLINE'

export function useEventStream(maxEvents = 100) {
  const [events, setEvents] = useState<StreamEvent[]>([])
  const [connectionState, setConnectionState] = useState<ConnectionState>('OFFLINE')
  const [lastEventTime, setLastEventTime] = useState<string | null>(null)
  const eventSourceRef = useRef<EventSource | null>(null)

  useEffect(() => {
    let reconnectTimer: ReturnType<typeof setTimeout>
    let isMounted = true

    const connect = async () => {
      if (!isMounted) return
      setConnectionState('RECONNECTING')
      let token = ''
      try {
        const ticket = await getSSETicket()
        token = ticket.token
      } catch {
        // Fallback for open auth mode
      }

      if (!isMounted) return

      const url = token ? `/api/events/stream?token=${encodeURIComponent(token)}` : '/api/events/stream'
      const es = new EventSource(url)
      eventSourceRef.current = es

      es.onopen = () => {
        if (isMounted) setConnectionState('LIVE')
      }

      es.onerror = () => {
        if (!isMounted) return
        setConnectionState('RECONNECTING')
        es.close()
        reconnectTimer = setTimeout(connect, 3000)
      }

      const handleMessage = (e: MessageEvent) => {
        try {
          const parsed: StreamEvent = JSON.parse(e.data)
          if (parsed && parsed.type && isMounted) {
            setLastEventTime(new Date().toLocaleTimeString())
            setEvents(prev => [parsed, ...prev].slice(0, maxEvents))
          }
        } catch {
          // ignore non-JSON keepalives
        }
      }

      es.onmessage = handleMessage

      const eventTypes = [
        'CONNECT', 'NODE_JOINED', 'NODE_STATUS_CHANGED', 'NODE_CRASHED', 'NODE_UNAVAILABLE', 'NODE_RECOVERED',
        'PUT_OBJECT', 'GET_OBJECT', 'DELETE_OBJECT', 'TOMBSTONE_WRITTEN',
        'REPLICA_CREATED', 'WRITE_QUORUM_REACHED', 'READ_QUORUM_REACHED',
        'WAL_APPEND', 'WAL_FSYNC', 'CHECKSUM_VERIFIED', 'CHECKSUM_MISMATCH',
        'SCRUB_STARTED', 'SCRUB_COMPLETED', 'REPAIR_QUEUED', 'REPAIR_STARTED', 'REPAIR_COMPLETED', 'REPAIR_FAILED',
        'RAFT_LEADER_CHANGED', 'RAFT_TERM_CHANGED', 'EPOCH_CHANGED', 'STALE_EPOCH_REJECTED'
      ]

      eventTypes.forEach(t => {
        es.addEventListener(t, handleMessage as EventListener)
      })
    }

    connect()

    return () => {
      isMounted = false
      if (reconnectTimer) clearTimeout(reconnectTimer)
      if (eventSourceRef.current) {
        eventSourceRef.current.close()
      }
    }
  }, [maxEvents])

  return { events, connectionState, lastEventTime }
}
