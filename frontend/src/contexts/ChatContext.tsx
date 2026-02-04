'use client'

import React, { createContext, useContext, useState, ReactNode } from 'react'

interface Incident {
  id: string
  title: string
  description: string
  severity: string
  status: string
}

interface ChatContextType {
  currentIncident: Incident | null
  setCurrentIncident: (incident: Incident | null) => void
  isOpen: boolean
  setIsOpen: (open: boolean) => void
}

const ChatContext = createContext<ChatContextType | undefined>(undefined)

export function ChatProvider({ children }: { children: ReactNode }) {
  const [currentIncident, setCurrentIncident] = useState<Incident | null>(null)
  const [isOpen, setIsOpen] = useState(false)

  return (
    <ChatContext.Provider
      value={{
        currentIncident,
        setCurrentIncident,
        isOpen,
        setIsOpen,
      }}
    >
      {children}
    </ChatContext.Provider>
  )
}

export function useChatContext() {
  const context = useContext(ChatContext)
  if (context === undefined) {
    // Return a default value instead of throwing to avoid build issues
    return {
      currentIncident: null,
      setCurrentIncident: () => {},
      isOpen: false,
      setIsOpen: () => {},
    }
  }
  return context
}

export default ChatContext
