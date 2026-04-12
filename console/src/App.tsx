import { BrowserRouter, Routes, Route, Navigate } from "react-router-dom";
import { AppShell } from "@/components/layout/AppShell";

function DashboardPlaceholder() {
  return <div className="text-muted-foreground">Dashboard loading...</div>;
}

export default function App() {
  return (
    <BrowserRouter basename="/ui">
      <AppShell>
        <Routes>
          <Route path="/" element={<Navigate to="/dashboard" replace />} />
          <Route path="/dashboard" element={<DashboardPlaceholder />} />
          <Route path="/browser" element={<div className="text-muted-foreground">KV Browser — Phase 130</div>} />
          <Route path="/console" element={<div className="text-muted-foreground">Query Console — Phase 131</div>} />
          <Route path="/vectors" element={<div className="text-muted-foreground">Vector Explorer — Phase 132</div>} />
          <Route path="/graph" element={<div className="text-muted-foreground">Graph Explorer — Phase 133</div>} />
          <Route path="/memory" element={<div className="text-muted-foreground">Memory — Phase 134</div>} />
        </Routes>
      </AppShell>
    </BrowserRouter>
  );
}
