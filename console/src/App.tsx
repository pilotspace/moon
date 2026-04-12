import { useEffect, lazy, Suspense } from "react";
import { BrowserRouter, Routes, Route, Navigate } from "react-router-dom";
import { AppShell } from "@/components/layout/AppShell";
import { Dashboard } from "@/views/Dashboard";
import { Browser } from "@/views/Browser";
import { connectSSE, disconnectSSE } from "@/lib/sse";

const Console = lazy(() =>
  import("@/views/Console").then((m) => ({ default: m.Console })),
);

const VectorExplorer = lazy(() =>
  import("@/views/VectorExplorer").then((m) => ({ default: m.VectorExplorer })),
);

const GraphExplorer = lazy(() =>
  import("@/views/GraphExplorer").then((m) => ({ default: m.GraphExplorer })),
);

const MemoryView = lazy(() =>
  import("@/views/MemoryView").then((m) => ({ default: m.MemoryView })),
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
          <Route path="/vectors" element={
            <Suspense fallback={<div className="text-muted-foreground p-8">Loading Vector Explorer...</div>}>
              <VectorExplorer />
            </Suspense>
          } />
          <Route path="/graph" element={
            <Suspense fallback={<div className="text-muted-foreground p-8">Loading Graph Explorer...</div>}>
              <GraphExplorer />
            </Suspense>
          } />
          <Route path="/memory" element={
            <Suspense fallback={<div className="text-muted-foreground p-8">Loading Memory view...</div>}>
              <MemoryView />
            </Suspense>
          } />
        </Routes>
      </AppShell>
    </BrowserRouter>
  );
}
