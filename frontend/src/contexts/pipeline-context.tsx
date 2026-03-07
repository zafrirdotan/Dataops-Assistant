"use client";

import {
  createContext,
  useContext,
  useState,
  useCallback,
  useEffect,
  ReactNode,
  useRef,
} from "react";
import { useAuth } from "@/hooks/useAuth";

export type Pipeline = {
  id: number;
  pipeline_id: string;
  name: string;
  created_by: string;
  description: string;
  created_at: string;
  updated_at: string | null;
  status: "draft" | "deployed" | string;
  spec: {
    schedule?: string;
    [key: string]: any;
  };
  image_id: string | null;
};

type PipelineContextType = {
  pipelines: Pipeline[];
  loading: boolean;
  error: string | null;
  refreshPipelines: () => void;
};

const PipelineContext = createContext<PipelineContextType | undefined>(
  undefined,
);

const API_URL = process.env.NEXT_PUBLIC_API_URL || "http://localhost:8080";

export function PipelineProvider({ children }: { children: ReactNode }) {
  const [pipelines, setPipelines] = useState<Pipeline[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const debounceTimerRef = useRef<NodeJS.Timeout | null>(null);
  const hasDataRef = useRef(false);
  const { isAuthenticated, loading: authLoading } = useAuth();
  const prevAuthenticatedRef = useRef<boolean | null>(null);

  const fetchPipelines = useCallback(async () => {
    try {
      // Only show full loading on initial load (no data yet); refetches keep list visible
      if (!hasDataRef.current) {
        setLoading(true);
      }
      setError(null);

      const response = await fetch(`${API_URL}/pipelines`, {
        method: "GET",
        credentials: "include",
      });

      if (response.ok) {
        const data = await response.json();
        setPipelines(data);
        hasDataRef.current = true;
      } else if (response.status === 401 || response.status === 403) {
        // User not authenticated, set empty pipelines
        setPipelines([]);
        hasDataRef.current = true;
      } else {
        throw new Error(`Failed to fetch pipelines: ${response.statusText}`);
      }
    } catch (err) {
      console.error("Failed to fetch pipelines:", err);
      setError(
        err instanceof Error ? err.message : "Failed to fetch pipelines",
      );
      setPipelines([]);
    } finally {
      setLoading(false);
    }
  }, []);

  // Debounced refresh to prevent duplicate calls
  const refreshPipelines = useCallback(() => {
    if (debounceTimerRef.current) {
      clearTimeout(debounceTimerRef.current);
    }

    debounceTimerRef.current = setTimeout(() => {
      fetchPipelines();
    }, 300); // 300ms debounce
  }, [fetchPipelines]);

  // Initial fetch on mount
  useEffect(() => {
    fetchPipelines();
  }, [fetchPipelines]);

  // Refetch when user becomes authenticated (e.g. after login) so pipelines appear
  useEffect(() => {
    if (authLoading) return;
    const wasAuthenticated = prevAuthenticatedRef.current;
    prevAuthenticatedRef.current = isAuthenticated;
    if (wasAuthenticated === false && isAuthenticated) {
      refreshPipelines();
    }
  }, [authLoading, isAuthenticated, refreshPipelines]);

  // Cleanup debounce timer
  useEffect(() => {
    return () => {
      if (debounceTimerRef.current) {
        clearTimeout(debounceTimerRef.current);
      }
    };
  }, []);

  return (
    <PipelineContext.Provider
      value={{ pipelines, loading, error, refreshPipelines }}
    >
      {children}
    </PipelineContext.Provider>
  );
}

export function usePipelineContext() {
  const context = useContext(PipelineContext);
  if (context === undefined) {
    throw new Error(
      "usePipelineContext must be used within a PipelineProvider",
    );
  }
  return context;
}
