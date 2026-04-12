import { NavLink } from "react-router-dom";
import {
  LayoutDashboard,
  Database,
  Terminal,
  Hexagon,
  GitFork,
  MemoryStick,
} from "lucide-react";
import { cn } from "@/lib/utils";
import { useMetricsStore } from "@/stores/metrics";

const navItems = [
  { to: "/dashboard", icon: LayoutDashboard, label: "Dashboard" },
  { to: "/browser", icon: Database, label: "Browser" },
  { to: "/console", icon: Terminal, label: "Console" },
  { to: "/vectors", icon: Hexagon, label: "Vectors" },
  { to: "/graph", icon: GitFork, label: "Graph" },
  { to: "/memory", icon: MemoryStick, label: "Memory" },
];

export function Sidebar() {
  const connected = useMetricsStore((s) => s.connected);

  return (
    <aside className="flex w-56 flex-col border-r border-border bg-zinc-950">
      {/* Logo */}
      <div className="flex items-center gap-2 px-4 py-4 border-b border-border">
        <div className="h-7 w-7 rounded-full bg-primary flex items-center justify-center text-xs font-bold text-primary-foreground">
          M
        </div>
        <span className="text-sm font-semibold tracking-tight">Moon Console</span>
      </div>

      {/* Navigation */}
      <nav className="flex-1 space-y-1 px-2 py-3">
        {navItems.map(({ to, icon: Icon, label }) => (
          <NavLink
            key={to}
            to={to}
            className={({ isActive }) =>
              cn(
                "flex items-center gap-3 rounded-md px-3 py-2 text-sm font-medium transition-colors",
                isActive
                  ? "bg-accent text-accent-foreground"
                  : "text-muted-foreground hover:bg-accent/50 hover:text-accent-foreground"
              )
            }
          >
            <Icon className="h-4 w-4" />
            {label}
          </NavLink>
        ))}
      </nav>

      {/* Connection status */}
      <div className="border-t border-border px-4 py-3">
        <div className="flex items-center gap-2 text-xs">
          <div
            className={cn(
              "h-2 w-2 rounded-full",
              connected ? "bg-success" : "bg-destructive"
            )}
          />
          <span className="text-muted-foreground">
            {connected ? "Connected" : "Disconnected"}
          </span>
        </div>
      </div>
    </aside>
  );
}
