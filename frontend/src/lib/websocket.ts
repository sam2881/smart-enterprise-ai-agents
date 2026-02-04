import io, { Socket } from 'socket.io-client'
import { WS_URL, WEBSOCKET_EVENTS } from './constants'

class WebSocketClient {
  private socket: Socket | null = null
  private listeners: Map<string, Set<Function>> = new Map()

  connect() {
    if (this.socket?.connected) {
      return this.socket
    }

    this.socket = io(WS_URL, {
      transports: ['websocket'],
      reconnection: true,
      reconnectionDelay: 1000,
      reconnectionDelayMax: 5000,
      reconnectionAttempts: 5,
    })

    this.socket.on('connect', () => {
      console.log('WebSocket connected')
    })

    this.socket.on('disconnect', () => {
      console.log('WebSocket disconnected')
    })

    this.socket.on('error', (error) => {
      console.error('WebSocket error:', error)
    })

    // Set up event listeners for all registered callbacks
    Object.values(WEBSOCKET_EVENTS).forEach((event) => {
      this.socket?.on(event, (data) => {
        const callbacks = this.listeners.get(event)
        if (callbacks) {
          callbacks.forEach((callback) => callback(data))
        }
      })
    })

    return this.socket
  }

  disconnect() {
    if (this.socket) {
      this.socket.disconnect()
      this.socket = null
    }
  }

  on(event: string, callback: Function) {
    if (!this.listeners.has(event)) {
      this.listeners.set(event, new Set())
    }
    this.listeners.get(event)?.add(callback)

    // If socket is already connected, add listener immediately
    if (this.socket?.connected) {
      this.socket.on(event, callback as any)
    }

    // Return unsubscribe function
    return () => {
      this.off(event, callback)
    }
  }

  off(event: string, callback: Function) {
    const callbacks = this.listeners.get(event)
    if (callbacks) {
      callbacks.delete(callback)
      if (callbacks.size === 0) {
        this.listeners.delete(event)
      }
    }

    if (this.socket) {
      this.socket.off(event, callback as any)
    }
  }

  emit(event: string, data: any) {
    if (this.socket?.connected) {
      this.socket.emit(event, data)
    } else {
      console.warn('WebSocket not connected, cannot emit event:', event)
    }
  }

  isConnected(): boolean {
    return this.socket?.connected || false
  }
}

export const wsClient = new WebSocketClient()
