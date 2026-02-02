"use client";

import { useAuth } from "@/hooks/useAuth";
import { Button } from "@/components/ui/button";
import Link from "next/link";
import { LogOut, User } from "lucide-react";

type AppHeaderProps = {
  title?: string;
  subtitle?: string;
};

export function AppHeader({
  title = "DataOps Assistant",
  subtitle = "Streaming ETL assistant with live pipeline steps.",
}: AppHeaderProps) {
  const { user, logout, isAuthenticated } = useAuth();

  return (
    <header
      className="flex items-center justify-between py-4 pr-4"
      aria-label="Application header"
    >
      <div>
        <h1 className="text-xl font-semibold">{title}</h1>
        <p className="text-xs text-zinc-500">{subtitle}</p>
      </div>

      <div className="flex items-center gap-4">
        {isAuthenticated ? (
          <>
            <Link href="/profile">
              <Button variant="ghost" size="sm" className="gap-2">
                <User size={16} />
                {user?.username}
              </Button>
            </Link>
            <Button
              variant="outline"
              size="sm"
              onClick={logout}
              className="gap-2"
            >
              <LogOut size={16} />
              Logout
            </Button>
          </>
        ) : (
          <>
            <Link href="/login">
              <Button variant="outline" size="sm">
                Login
              </Button>
            </Link>
            <Link href="/signup">
              <Button size="sm">Sign Up</Button>
            </Link>
          </>
        )}
      </div>
    </header>
  );
}
