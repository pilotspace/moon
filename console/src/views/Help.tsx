import {
  BookOpen,
  Terminal as TerminalIcon,
  Database,
  Hexagon,
  GitFork,
  MemoryStick,
  LayoutDashboard,
  Keyboard,
  Zap,
  AlertCircle,
} from "lucide-react";

/**
 * Help view — built-in guide for users interacting with Moon via the console.
 * Covers connection, per-view workflows, command cheatsheet, and shortcuts.
 */
export function Help() {
  return (
    <div className="mx-auto max-w-5xl space-y-10 px-6 py-6">
      {/* Hero */}
      <header className="space-y-2">
        <div className="flex items-center gap-3">
          <BookOpen className="h-6 w-6 text-primary" />
          <h1 className="text-2xl font-semibold tracking-tight">
            Moon Console Guide
          </h1>
        </div>
        <p className="max-w-3xl text-sm text-muted-foreground">
          Moon is a Redis-compatible data engine with vector search and graph
          queries. This console lets you browse keys, run commands, and
          visualize vectors and graphs — all from your browser. No Redis
          knowledge required to get started.
        </p>
      </header>

      {/* Quick Start */}
      <Section icon={<Zap className="h-4 w-4" />} title="Quick Start">
        <ol className="ml-4 list-decimal space-y-2 text-sm leading-relaxed text-muted-foreground">
          <li>
            <strong className="text-foreground">Connect a client.</strong> Any
            Redis client works. From your terminal:
            <Code>redis-cli -p 6399</Code>
            Or via Python:
            <Code>
              {`import redis
r = redis.Redis(host="localhost", port=6399)
r.set("hello", "world")`}
            </Code>
          </li>
          <li>
            <strong className="text-foreground">Populate some data.</strong> A
            quick sample:
            <Code>
              {`redis-cli -p 6399 SET user:1:name "Alice"
redis-cli -p 6399 HSET user:1:profile email alice@example.com role engineer
redis-cli -p 6399 LPUSH queue:jobs job1 job2 job3
redis-cli -p 6399 SADD tags rust redis moon
redis-cli -p 6399 ZADD leaderboard 100 alice 85 bob`}
            </Code>
          </li>
          <li>
            <strong className="text-foreground">Browse it.</strong> Head to the{" "}
            <InlineLink href="/browser">Browser</InlineLink>. Keys are grouped
            by <code className="text-xs">:</code> delimiters into namespaces
            (e.g., <code className="text-xs">user:1:name</code> → under{" "}
            <code className="text-xs">user / 1 / name</code>).
          </li>
          <li>
            <strong className="text-foreground">Run queries.</strong> Open the{" "}
            <InlineLink href="/console">Console</InlineLink> — a Monaco editor
            with autocomplete for 230+ commands. Press{" "}
            <Kbd>Cmd/Ctrl+Enter</Kbd> to execute.
          </li>
        </ol>
      </Section>

      {/* View guides */}
      <Section icon={<BookOpen className="h-4 w-4" />} title="Views">
        <div className="grid gap-3 md:grid-cols-2">
          <ViewCard
            icon={<LayoutDashboard className="h-4 w-4" />}
            name="Dashboard"
            href="/dashboard"
            desc="Live server metrics via Server-Sent Events. Ops/sec, memory, connections, keyspace, cache hit ratio, slowlog. Updates at 1 Hz."
            tip="If 'Connected' is green at the bottom of the sidebar, metrics are streaming."
          />
          <ViewCard
            icon={<Database className="h-4 w-4" />}
            name="Browser"
            href="/browser"
            desc="Virtual-scrolled key browser with namespace tree. Click a key to inspect and edit. Supports Strings, Hashes, Lists, Sets, Sorted Sets, Streams."
            tip="Filter by pattern, type, or TTL. Bulk-select with checkboxes for multi-delete."
          />
          <ViewCard
            icon={<TerminalIcon className="h-4 w-4" />}
            name="Console"
            href="/console"
            desc="Monaco editor with syntax highlighting for RESP commands and Cypher queries. Multi-tab, history, autocomplete."
            tip="Cmd+Enter to run. Cmd+T for a new tab. Use GRAPH.QUERY for Cypher, FT.SEARCH for vectors."
          />
          <ViewCard
            icon={<Hexagon className="h-4 w-4" />}
            name="Vectors"
            href="/vectors"
            desc="3D UMAP projection of your vector index. Up to 50K points rendered interactively with HNSW overlay."
            tip="Create an index first: FT.CREATE my_idx ... then HSET keys with a vector field."
          />
          <ViewCard
            icon={<GitFork className="h-4 w-4" />}
            name="Graph"
            href="/graph"
            desc="3D force-directed graph layout for Cypher query results. Click nodes to see properties and degree."
            tip="Try MATCH (n)-[r]->(m) RETURN n, r, m LIMIT 200 to see everything."
          />
          <ViewCard
            icon={<MemoryStick className="h-4 w-4" />}
            name="Memory"
            href="/memory"
            desc="Treemap of keyspace memory by namespace and data type. Slowlog viewer. Command statistics ranked by CPU time."
            tip="Click a treemap rectangle to drill into a namespace prefix."
          />
        </div>
      </Section>

      {/* Command cheatsheet */}
      <Section icon={<TerminalIcon className="h-4 w-4" />} title="Command Cheatsheet">
        <div className="grid gap-4 md:grid-cols-3">
          <CmdGroup
            title="Strings & Keys"
            rows={[
              ["SET k v", "Set a string value"],
              ["GET k", "Read a string"],
              ["EXPIRE k 60", "Set 60s TTL"],
              ["DEL k1 k2", "Delete keys"],
              ["SCAN 0 MATCH pre:*", "Paginated key scan"],
              ["TYPE k", "Get data type of k"],
            ]}
          />
          <CmdGroup
            title="Collections"
            rows={[
              ["HSET k f v", "Hash field"],
              ["LPUSH k v", "List push-left"],
              ["SADD k m", "Set add member"],
              ["ZADD k 10 m", "Sorted set score"],
              ["XADD s * k v", "Stream entry"],
              ["HGETALL k", "All hash fields"],
            ]}
          />
          <CmdGroup
            title="Vectors & Graph"
            rows={[
              ["FT.CREATE idx ...", "Vector index"],
              ["FT.SEARCH idx q KNN 10", "KNN search"],
              ["FT.INFO idx", "Index metadata"],
              ["GRAPH.QUERY g 'MATCH...'", "Cypher query"],
              ["GRAPH.INFO g", "Graph metadata"],
              ["GRAPH.LIST", "All graphs"],
            ]}
          />
        </div>
      </Section>

      {/* Keyboard shortcuts */}
      <Section icon={<Keyboard className="h-4 w-4" />} title="Keyboard Shortcuts">
        <div className="grid gap-4 md:grid-cols-2">
          <ShortcutGroup
            title="Console"
            rows={[
              [<><Kbd>Cmd</Kbd>+<Kbd>Enter</Kbd></>, "Run current line (or selection)"],
              [<><Kbd>Cmd</Kbd>+<Kbd>Shift</Kbd>+<Kbd>Enter</Kbd></>, "Run all lines (batch)"],
              [<><Kbd>Cmd</Kbd>+<Kbd>T</Kbd></>, "New tab"],
              [<><Kbd>Cmd</Kbd>+<Kbd>W</Kbd></>, "Close tab"],
              [<><Kbd>Cmd</Kbd>+<Kbd>↑</Kbd></>, "Previous history entry"],
              [<><Kbd>Cmd</Kbd>+<Kbd>↓</Kbd></>, "Next history entry"],
              [<><Kbd>Cmd</Kbd>+<Kbd>H</Kbd></>, "Toggle history panel"],
            ]}
          />
          <ShortcutGroup
            title="Navigation"
            rows={[
              [<><Kbd>G</Kbd><Kbd>D</Kbd></>, "Go to Dashboard (coming soon)"],
              [<><Kbd>G</Kbd><Kbd>B</Kbd></>, "Go to Browser (coming soon)"],
              ["Click sidebar", "Navigate between views"],
              ["Click namespace", "Filter by prefix"],
              ["Click key row", "Open value editor"],
              ["Ctrl-click point", "Select in 3D view"],
            ]}
          />
        </div>
      </Section>

      {/* Query examples */}
      <Section icon={<BookOpen className="h-4 w-4" />} title="Query Examples">
        <div className="space-y-4">
          <Example
            title="Create and query a vector index"
            body={`FT.CREATE embeddings SCHEMA v VECTOR HNSW 6 TYPE FLOAT32 DIM 384 DISTANCE_METRIC COSINE

HSET doc:1 v "$(printf '%.0s\\x00' {1..1536})"

FT.SEARCH embeddings "*=>[KNN 10 @v $query]" PARAMS 2 query "..." DIALECT 2`}
          />
          <Example
            title="Build a knowledge graph"
            body={`GRAPH.QUERY kg "CREATE (a:Person {name:'Alice'})-[:KNOWS]->(b:Person {name:'Bob'})"

GRAPH.QUERY kg "MATCH (p:Person)-[:KNOWS]->(friend) WHERE p.name = 'Alice' RETURN friend.name"

GRAPH.INFO kg`}
          />
          <Example
            title="Session store with TTL"
            body={`HSET session:abc123 user u42 ip 10.0.0.1 started $(date +%s)
EXPIRE session:abc123 3600
TTL session:abc123`}
          />
        </div>
      </Section>

      {/* FAQ */}
      <Section
        icon={<AlertCircle className="h-4 w-4" />}
        title="Tips & Gotchas"
      >
        <div className="space-y-3 text-sm text-muted-foreground">
          <Faq q="Why do I see only some of my keys in the Browser?">
            Moon uses a thread-per-core shared-nothing model with per-shard
            DashTables. The REST SCAN API currently queries one shard. Run with{" "}
            <code className="text-xs">--shards 1</code> for a unified keyspace
            in the UI, or populate keys with{" "}
            <code className="text-xs">{`{hashtag}`}</code> to co-locate them.
          </Faq>
          <Faq q="Why is key size shown as 0 B?">
            The treemap uses <code className="text-xs">MEMORY USAGE</code>. If
            your Moon build doesn't expose that command yet, byte sizes will
            read as 0. The structure (namespaces, counts, types) still renders
            correctly.
          </Faq>
          <Faq q="How do I connect from outside the server host?">
            The admin port (default 9100) binds to the address passed via{" "}
            <code className="text-xs">--bind</code>. For LAN access, start Moon
            with <code className="text-xs">--bind 0.0.0.0</code>. Protect the
            admin port in production — it exposes a full command gateway.
          </Faq>
          <Faq q="Is the console safe for production?">
            v0.1.5 is an operator tool. The HTTP gateway dispatches any RESP
            command to the server. Keep the admin port behind your private
            network or firewall. ACL-aware gateway auth is a v0.2+ item.
          </Faq>
          <Faq q="What's the keyboard shortcut for X?">
            See the Keyboard Shortcuts section above. Console editor shortcuts
            use Monaco defaults; hover over buttons for tooltips.
          </Faq>
        </div>
      </Section>
    </div>
  );
}

function Section({
  icon,
  title,
  children,
}: {
  icon: React.ReactNode;
  title: string;
  children: React.ReactNode;
}) {
  return (
    <section className="space-y-3">
      <h2 className="flex items-center gap-2 border-b border-border pb-2 text-base font-semibold">
        <span className="text-primary">{icon}</span>
        {title}
      </h2>
      <div className="pt-1">{children}</div>
    </section>
  );
}

function ViewCard({
  icon,
  name,
  href,
  desc,
  tip,
}: {
  icon: React.ReactNode;
  name: string;
  href: string;
  desc: string;
  tip: string;
}) {
  return (
    <a
      href={`/ui${href}`}
      className="group block rounded-lg border border-border bg-card p-4 transition-colors hover:border-primary hover:bg-accent/30"
    >
      <div className="mb-1 flex items-center gap-2 text-sm font-semibold text-foreground group-hover:text-primary">
        <span className="text-primary">{icon}</span>
        {name}
      </div>
      <p className="mb-2 text-xs text-muted-foreground">{desc}</p>
      <p className="text-xs text-muted-foreground">
        <span className="font-medium text-foreground">Tip:</span> {tip}
      </p>
    </a>
  );
}

function CmdGroup({
  title,
  rows,
}: {
  title: string;
  rows: [string, string][];
}) {
  return (
    <div className="rounded-lg border border-border bg-card p-3">
      <h3 className="mb-2 text-xs font-semibold uppercase tracking-wide text-muted-foreground">
        {title}
      </h3>
      <dl className="space-y-1.5 text-xs">
        {rows.map(([cmd, desc]) => (
          <div key={cmd} className="flex flex-col gap-0.5">
            <code className="font-mono text-primary">{cmd}</code>
            <span className="text-muted-foreground">{desc}</span>
          </div>
        ))}
      </dl>
    </div>
  );
}

function ShortcutGroup({
  title,
  rows,
}: {
  title: string;
  rows: [React.ReactNode, string][];
}) {
  return (
    <div className="rounded-lg border border-border bg-card p-3">
      <h3 className="mb-2 text-xs font-semibold uppercase tracking-wide text-muted-foreground">
        {title}
      </h3>
      <dl className="space-y-1.5 text-xs">
        {rows.map(([keys, desc], i) => (
          <div key={i} className="flex items-center justify-between gap-3">
            <span className="flex items-center gap-1">{keys}</span>
            <span className="text-muted-foreground">{desc}</span>
          </div>
        ))}
      </dl>
    </div>
  );
}

function Example({ title, body }: { title: string; body: string }) {
  return (
    <div className="rounded-lg border border-border bg-card">
      <div className="border-b border-border px-3 py-2 text-xs font-semibold text-foreground">
        {title}
      </div>
      <Code block>{body}</Code>
    </div>
  );
}

function Code({
  children,
  block,
}: {
  children: React.ReactNode;
  block?: boolean;
}) {
  return (
    <pre
      className={
        "overflow-x-auto text-xs leading-relaxed font-mono " +
        (block
          ? "bg-transparent p-3 text-zinc-200"
          : "my-2 rounded bg-zinc-900/60 p-2 text-zinc-200")
      }
    >
      <code>{children}</code>
    </pre>
  );
}

function Kbd({ children }: { children: React.ReactNode }) {
  return (
    <kbd className="rounded border border-border bg-muted px-1.5 py-0.5 text-[10px] font-mono text-foreground">
      {children}
    </kbd>
  );
}

function InlineLink({
  href,
  children,
}: {
  href: string;
  children: React.ReactNode;
}) {
  return (
    <a
      href={`/ui${href}`}
      className="text-primary underline-offset-2 hover:underline"
    >
      {children}
    </a>
  );
}

function Faq({ q, children }: { q: string; children: React.ReactNode }) {
  return (
    <div className="rounded-lg border border-border bg-card p-3">
      <div className="mb-1 text-sm font-medium text-foreground">{q}</div>
      <div className="text-xs">{children}</div>
    </div>
  );
}
