"use client";

import {
  createContext,
  useContext,
  useState,
  useCallback,
  type ReactNode,
} from "react";

type SidebarContextType = {
  refreshChatsTrigger: number;
  incrementRefreshChatsTrigger: () => void;
  onNewChat: () => void;
  setOnNewChat: (fn: () => void) => void;
};

const SidebarContext = createContext<SidebarContextType | undefined>(undefined);

export function SidebarProvider({ children }: { children: ReactNode }) {
  const [refreshChatsTrigger, setRefreshChatsTrigger] = useState(0);
  const [onNewChat, setOnNewChatState] = useState<() => void>(() => () => {});

  const incrementRefreshChatsTrigger = useCallback(() => {
    setRefreshChatsTrigger((t) => t + 1);
  }, []);

  const setOnNewChat = useCallback((fn: () => void) => {
    setOnNewChatState(() => fn);
  }, []);

  return (
    <SidebarContext.Provider
      value={{
        refreshChatsTrigger,
        incrementRefreshChatsTrigger,
        onNewChat,
        setOnNewChat,
      }}
    >
      {children}
    </SidebarContext.Provider>
  );
}

export function useSidebar() {
  const ctx = useContext(SidebarContext);
  if (ctx === undefined) {
    throw new Error("useSidebar must be used within SidebarProvider");
  }
  return ctx;
}
