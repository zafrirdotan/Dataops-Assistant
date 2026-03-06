"use client";

import { useEffect } from "react";
import { useRouter } from "next/navigation";
import {
    ResizablePanelGroup,
    ResizablePanel,
    ResizableHandle,
} from "@/components/ui/resizable";
import { SideNav } from "@/components/side-nav";
import { SidebarProvider, useSidebar } from "@/contexts/sidebar-context";
import { ProtectedRoute } from "@/components/auth/protected-route";

function AppLayoutContent({ children }: { children: React.ReactNode }) {
    const router = useRouter();
    const { refreshChatsTrigger, onNewChat, setOnNewChat } = useSidebar();

    // Ensure "New chat" always navigates to home from any page (e.g. when opening /c/[id] directly)
    useEffect(() => {
        setOnNewChat(() => router.replace("/"));
    }, [router, setOnNewChat]);

    return (
        <div className="h-screen overflow-hidden bg-white text-black flex flex-col">
            <div className="flex flex-1 min-h-0">
                <ResizablePanelGroup orientation="horizontal" className="flex-1 min-h-0">
                    <ResizablePanel defaultSize="20%" className="hidden lg:block min-w-0">
                        <aside className="h-full w-full">
                            <SideNav
                                onNewChat={onNewChat}
                                refreshChatsTrigger={refreshChatsTrigger}
                            />
                        </aside>
                    </ResizablePanel>
                    <ResizableHandle className="hidden lg:flex" withHandle />
                    <ResizablePanel
                        defaultSize="90%"
                        className="min-w-0 flex flex-col h-full min-h-0"
                    >

                        <main className="flex-1 min-h-0 flex flex-col overflow-hidden">
                            {children}
                        </main>
                    </ResizablePanel>
                </ResizablePanelGroup>
            </div>
        </div>
    );
}

export default function AppLayout({
    children,
}: {
    children: React.ReactNode;
}) {
    return (
        <ProtectedRoute>
            <SidebarProvider>
                <AppLayoutContent>{children}</AppLayoutContent>
            </SidebarProvider>
        </ProtectedRoute>
    );
}
