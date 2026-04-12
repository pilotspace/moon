import { useEffect, lazy, Suspense } from "react";
import { BrowserRouter, Routes, Route, Navigate } from "react-router-dom";
import { AppShell } from "@/components/layout/AppShell";
import { Dashboard } from "@/views/Dashboard";
import { Browser } from "@/views/Browser";
import { connectSSE, disconnectSSE } from "@/lib/sse";

const Console = lazy(() =>
  import("@/views/Console").then((m) => ({ default: m.Console })),
);

export default function App() {
  useEffect(() => {
    connectSSE();
    return () => disconnectSSE();
  }, []);

  return (
    <BrowserRouter basename="/ui">
      <AppShell>
        <Routes>
          <Route path="/" element={<Navigate to="/dashboard" replace />} />
          <Route path="/dashboard" element={<Dashboard />} />
          <Route path="/browser" element={<Browser />} />
          <Route path="/console" element={
            <Suspense fallback={<div className="text-muted-foreground p-8">Loading console...</div>}>
              <Console />
            </Suspense>
          } />
          <Route path="/vectors" element={<div className="text-muted-foreground p-8">Vector Explorer — Phase 132</div>} />
          <Route path="/graph" element={<div className="text-muted-foreground p-8">Graph Explorer — Phase 133</div>} />
          <Route path="/memory" element={<div className="text-muted-foreground p-8">Memory — Phase 134</div>} />
        </Routes>
      </AppShell>
    </BrowserRouter>
  );
}
