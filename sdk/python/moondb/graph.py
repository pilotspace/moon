"""Graph engine helpers for MoonDB (GRAPH.* commands).

Wraps GRAPH.CREATE, GRAPH.ADDNODE, GRAPH.ADDEDGE, GRAPH.DELETE,
GRAPH.NEIGHBORS, GRAPH.QUERY, GRAPH.RO_QUERY, GRAPH.EXPLAIN,
GRAPH.INFO, GRAPH.LIST, GRAPH.VSEARCH, GRAPH.HYBRID, GRAPH.PROFILE.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Sequence, Union

from .types import GraphEdge, GraphNode, QueryResult, encode_vector


class GraphCommands:
    """Graph command helpers.

    Not instantiated directly -- accessed via ``MoonClient.graph``.

    Example::

        from moondb import MoonClient
        client = MoonClient()
        client.graph.create("social")
        node_id = client.graph.add_node("social", "Person", name="Alice", age="30")
    """

    def __init__(self, client: Any) -> None:
        self._client = client

    def create(self, name: str) -> str:
        """Create a named graph.

        Args:
            name: Graph name.

        Returns:
            "OK" on success.

        Example::

            client.graph.create("knowledge")
        """
        return self._client.execute_command("GRAPH.CREATE", name)  # type: ignore[no-any-return]

    def delete(self, name: str) -> str:
        """Delete a named graph and all its nodes/edges.

        Args:
            name: Graph name.

        Returns:
            "OK" on success.

        Example::

            client.graph.delete("old_graph")
        """
        return self._client.execute_command("GRAPH.DELETE", name)  # type: ignore[no-any-return]

    def add_node(
        self,
        graph: str,
        label: str,
        *,
        vector_field: Optional[str] = None,
        vector: Optional[Union[Sequence[float], bytes]] = None,
        **properties: str,
    ) -> int:
        """Add a node to the graph.

        Args:
            graph: Graph name.
            label: Node label (e.g. "Person", "Document").
            vector_field: Optional vector field name for vector-enabled nodes.
            vector: Optional vector data (list of floats or bytes).
            **properties: Key-value properties to store on the node.

        Returns:
            The numeric node ID assigned by the server.

        Example::

            node_id = client.graph.add_node(
                "social", "Person", name="Alice", age="30"
            )
            # With vector:
            node_id = client.graph.add_node(
                "knowledge", "Document",
                vector_field="embedding", vector=[0.1, 0.2, ...],
                title="My Doc",
            )
        """
        args: list[Any] = ["GRAPH.ADDNODE", graph, label]

        for k, v in properties.items():
            args.extend([k, v])

        if vector_field and vector is not None:
            blob = vector if isinstance(vector, bytes) else encode_vector(vector)
            args.extend(["VECTOR", vector_field, blob])

        result = self._client.execute_command(*args)
        return int(result)

    def add_edge(
        self,
        graph: str,
        src_id: int,
        dst_id: int,
        edge_type: str,
        *,
        weight: Optional[float] = None,
        **properties: str,
    ) -> str:
        """Add an edge between two nodes.

        Args:
            graph: Graph name.
            src_id: Source node ID.
            dst_id: Destination node ID.
            edge_type: Relationship type (e.g. "KNOWS", "LINKS_TO").
            weight: Optional edge weight.
            **properties: Key-value properties on the edge.

        Returns:
            "OK" on success.

        Example::

            client.graph.add_edge("social", 1, 2, "KNOWS", weight=0.9)
        """
        args: list[Any] = ["GRAPH.ADDEDGE", graph, str(src_id), str(dst_id), edge_type]

        for k, v in properties.items():
            args.extend([k, v])

        if weight is not None:
            args.extend(["WEIGHT", str(weight)])

        return self._client.execute_command(*args)  # type: ignore[no-any-return]

    def neighbors(
        self,
        graph: str,
        node_id: int,
        *,
        edge_type: Optional[str] = None,
        depth: int = 1,
    ) -> List[GraphNode]:
        """Get neighbors of a node.

        Args:
            graph: Graph name.
            node_id: Node ID to query.
            edge_type: Optional edge type filter.
            depth: Number of hops (default 1).

        Returns:
            List of GraphNode representing neighbors.

        Example::

            neighbors = client.graph.neighbors("social", 1, depth=2)
            for n in neighbors:
                print(n.node_id, n.label, n.properties)
        """
        args: list[Any] = ["GRAPH.NEIGHBORS", graph, str(node_id)]
        if edge_type:
            args.extend(["TYPE", edge_type])
        if depth > 1:
            args.extend(["DEPTH", str(depth)])

        raw = self._client.execute_command(*args)
        return _parse_neighbors(raw)

    def query(self, graph: str, cypher: str) -> QueryResult:
        """Execute a Cypher query (read-write).

        Args:
            graph: Graph name.
            cypher: Cypher query string.

        Returns:
            QueryResult with headers, rows, and stats.

        Example::

            result = client.graph.query(
                "social",
                "CREATE (a:Person {name: 'Alice'})-[:KNOWS]->(b:Person {name: 'Bob'}) RETURN a, b"
            )
        """
        raw = self._client.execute_command("GRAPH.QUERY", graph, cypher)
        return _parse_query_result(raw)

    def ro_query(self, graph: str, cypher: str) -> QueryResult:
        """Execute a read-only Cypher query (rejects writes).

        Args:
            graph: Graph name.
            cypher: Cypher query string (must not contain CREATE/DELETE/SET/MERGE).

        Returns:
            QueryResult with headers, rows, and stats.

        Example::

            result = client.graph.ro_query("social", "MATCH (n:Person) RETURN n.name")
        """
        raw = self._client.execute_command("GRAPH.RO_QUERY", graph, cypher)
        return _parse_query_result(raw)

    def explain(self, graph: str, cypher: str) -> List[str]:
        """Get the query execution plan without executing.

        Args:
            graph: Graph name.
            cypher: Cypher query string.

        Returns:
            List of plan step descriptions.

        Example::

            plan = client.graph.explain("social", "MATCH (n) RETURN n LIMIT 10")
            for step in plan:
                print(step)
        """
        raw = self._client.execute_command("GRAPH.EXPLAIN", graph, cypher)
        if isinstance(raw, list):
            return [_to_str(x) for x in raw]
        return [_to_str(raw)]

    def profile(self, graph: str, cypher: str) -> List[str]:
        """Execute query and return execution plan with timing stats.

        Args:
            graph: Graph name.
            cypher: Cypher query string.

        Returns:
            List of plan step descriptions with timing.

        Example::

            profile = client.graph.profile("social", "MATCH (n) RETURN n LIMIT 10")
        """
        raw = self._client.execute_command("GRAPH.PROFILE", graph, cypher)
        if isinstance(raw, list):
            return [_to_str(x) for x in raw]
        return [_to_str(raw)]

    def info(self, graph: str) -> Dict[str, Any]:
        """Get graph metadata (node/edge counts, labels, etc.).

        Args:
            graph: Graph name.

        Returns:
            Dictionary of graph metadata.

        Example::

            info = client.graph.info("social")
            print(info.get("num_nodes"), info.get("num_edges"))
        """
        raw = self._client.execute_command("GRAPH.INFO", graph)
        return _parse_info(raw)

    def list_graphs(self) -> List[str]:
        """List all named graphs.

        Returns:
            List of graph names.

        Example::

            graphs = client.graph.list_graphs()
            # ["social", "knowledge"]
        """
        raw = self._client.execute_command("GRAPH.LIST")
        if isinstance(raw, list):
            return [_to_str(x) for x in raw]
        return []

    def vsearch(
        self,
        graph: str,
        start_node_id: int,
        hops: int,
        k: int,
        vector: Union[Sequence[float], bytes],
        *,
        threshold: Optional[float] = None,
        edge_type: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """Vector-guided graph search starting from a node.

        Uses GRAPH.VSEARCH to traverse the graph guided by vector similarity.

        Args:
            graph: Graph name.
            start_node_id: Starting node ID.
            hops: Number of graph hops.
            k: Number of results.
            vector: Query vector.
            threshold: Optional distance threshold.
            edge_type: Optional edge type filter.

        Returns:
            List of result dictionaries with node IDs and scores.

        Example::

            results = client.graph.vsearch(
                "knowledge", 1, hops=3, k=5, vector=[0.1, 0.2, ...]
            )
        """
        blob = vector if isinstance(vector, bytes) else encode_vector(vector)
        args: list[Any] = [
            "GRAPH.VSEARCH", graph, str(start_node_id),
            str(hops), str(k), blob,
        ]
        if threshold is not None:
            args.extend(["THRESHOLD", str(threshold)])
        if edge_type:
            args.extend(["TYPE", edge_type])

        raw = self._client.execute_command(*args)
        return _parse_vsearch(raw)


class AsyncGraphCommands:
    """Async variant of GraphCommands for ``redis.asyncio.Redis``.

    All methods are async. See ``GraphCommands`` for documentation.

    Example::

        from moondb import AsyncMoonClient
        client = AsyncMoonClient()
        await client.graph.create("social")
        node_id = await client.graph.add_node("social", "Person", name="Alice")
    """

    def __init__(self, client: Any) -> None:
        self._client = client

    async def create(self, name: str) -> str:
        """Create a named graph (async)."""
        return await self._client.execute_command("GRAPH.CREATE", name)  # type: ignore[no-any-return]

    async def delete(self, name: str) -> str:
        """Delete a named graph (async)."""
        return await self._client.execute_command("GRAPH.DELETE", name)  # type: ignore[no-any-return]

    async def add_node(
        self,
        graph: str,
        label: str,
        *,
        vector_field: Optional[str] = None,
        vector: Optional[Union[Sequence[float], bytes]] = None,
        **properties: str,
    ) -> int:
        """Add a node (async). See ``GraphCommands.add_node``."""
        args: list[Any] = ["GRAPH.ADDNODE", graph, label]
        for k, v in properties.items():
            args.extend([k, v])
        if vector_field and vector is not None:
            blob = vector if isinstance(vector, bytes) else encode_vector(vector)
            args.extend(["VECTOR", vector_field, blob])
        result = await self._client.execute_command(*args)
        return int(result)

    async def add_edge(
        self,
        graph: str,
        src_id: int,
        dst_id: int,
        edge_type: str,
        *,
        weight: Optional[float] = None,
        **properties: str,
    ) -> str:
        """Add an edge (async). See ``GraphCommands.add_edge``."""
        args: list[Any] = ["GRAPH.ADDEDGE", graph, str(src_id), str(dst_id), edge_type]
        for k, v in properties.items():
            args.extend([k, v])
        if weight is not None:
            args.extend(["WEIGHT", str(weight)])
        return await self._client.execute_command(*args)  # type: ignore[no-any-return]

    async def neighbors(
        self,
        graph: str,
        node_id: int,
        *,
        edge_type: Optional[str] = None,
        depth: int = 1,
    ) -> List[GraphNode]:
        """Get neighbors (async). See ``GraphCommands.neighbors``."""
        args: list[Any] = ["GRAPH.NEIGHBORS", graph, str(node_id)]
        if edge_type:
            args.extend(["TYPE", edge_type])
        if depth > 1:
            args.extend(["DEPTH", str(depth)])
        raw = await self._client.execute_command(*args)
        return _parse_neighbors(raw)

    async def query(self, graph: str, cypher: str) -> QueryResult:
        """Execute Cypher query (async). See ``GraphCommands.query``."""
        raw = await self._client.execute_command("GRAPH.QUERY", graph, cypher)
        return _parse_query_result(raw)

    async def ro_query(self, graph: str, cypher: str) -> QueryResult:
        """Execute read-only Cypher query (async). See ``GraphCommands.ro_query``."""
        raw = await self._client.execute_command("GRAPH.RO_QUERY", graph, cypher)
        return _parse_query_result(raw)

    async def explain(self, graph: str, cypher: str) -> List[str]:
        """Get query plan (async). See ``GraphCommands.explain``."""
        raw = await self._client.execute_command("GRAPH.EXPLAIN", graph, cypher)
        if isinstance(raw, list):
            return [_to_str(x) for x in raw]
        return [_to_str(raw)]

    async def profile(self, graph: str, cypher: str) -> List[str]:
        """Execute with profiling (async). See ``GraphCommands.profile``."""
        raw = await self._client.execute_command("GRAPH.PROFILE", graph, cypher)
        if isinstance(raw, list):
            return [_to_str(x) for x in raw]
        return [_to_str(raw)]

    async def info(self, graph: str) -> Dict[str, Any]:
        """Get graph metadata (async). See ``GraphCommands.info``."""
        raw = await self._client.execute_command("GRAPH.INFO", graph)
        return _parse_info(raw)

    async def list_graphs(self) -> List[str]:
        """List all graphs (async). See ``GraphCommands.list_graphs``."""
        raw = await self._client.execute_command("GRAPH.LIST")
        if isinstance(raw, list):
            return [_to_str(x) for x in raw]
        return []

    async def vsearch(
        self,
        graph: str,
        start_node_id: int,
        hops: int,
        k: int,
        vector: Union[Sequence[float], bytes],
        *,
        threshold: Optional[float] = None,
        edge_type: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """Vector-guided graph search (async). See ``GraphCommands.vsearch``."""
        blob = vector if isinstance(vector, bytes) else encode_vector(vector)
        args: list[Any] = [
            "GRAPH.VSEARCH", graph, str(start_node_id),
            str(hops), str(k), blob,
        ]
        if threshold is not None:
            args.extend(["THRESHOLD", str(threshold)])
        if edge_type:
            args.extend(["TYPE", edge_type])
        raw = await self._client.execute_command(*args)
        return _parse_vsearch(raw)


# -- Response parsing helpers --


def _to_str(val: Any) -> str:
    """Convert bytes or string to str."""
    if isinstance(val, bytes):
        return val.decode("utf-8", errors="replace")
    return str(val)


def _parse_neighbors(raw: Any) -> List[GraphNode]:
    """Parse GRAPH.NEIGHBORS response.

    Expects a list of [node_id, label, [prop_key, prop_val, ...], ...]
    """
    if not isinstance(raw, list):
        return []

    nodes: List[GraphNode] = []
    for item in raw:
        if isinstance(item, list) and len(item) >= 2:
            node_id = int(item[0])
            label = _to_str(item[1])
            props: Dict[str, str] = {}
            if len(item) > 2 and isinstance(item[2], list):
                j = 0
                while j + 1 < len(item[2]):
                    props[_to_str(item[2][j])] = _to_str(item[2][j + 1])
                    j += 2
            nodes.append(GraphNode(node_id=node_id, label=label, properties=props))

    return nodes


def _parse_query_result(raw: Any) -> QueryResult:
    """Parse GRAPH.QUERY / GRAPH.RO_QUERY response.

    Response format varies -- typically:
    [headers_array, [row1, row2, ...], stats_array]
    """
    if not isinstance(raw, list) or len(raw) < 1:
        return QueryResult()

    headers: List[str] = []
    rows: List[List[Any]] = []
    stats: Dict[str, str] = {}

    # Parse headers (first element)
    if isinstance(raw[0], list):
        headers = [_to_str(h) for h in raw[0]]

    # Parse data rows (second element if present)
    if len(raw) > 1 and isinstance(raw[1], list):
        for row in raw[1]:
            if isinstance(row, list):
                rows.append([_to_str(v) for v in row])
            else:
                rows.append([_to_str(row)])

    # Parse stats (last element if it's a list of strings)
    if len(raw) > 2 and isinstance(raw[-1], list):
        for stat in raw[-1]:
            s = _to_str(stat)
            if ":" in s:
                k, _, v = s.partition(":")
                stats[k.strip()] = v.strip()

    return QueryResult(headers=headers, rows=rows, stats=stats)


def _parse_info(raw: Any) -> Dict[str, Any]:
    """Parse GRAPH.INFO response (flat key-value list)."""
    if not isinstance(raw, list):
        return {}

    info: Dict[str, Any] = {}
    i = 0
    while i + 1 < len(raw):
        key = _to_str(raw[i])
        val = raw[i + 1]
        if isinstance(val, bytes):
            info[key] = val.decode("utf-8", errors="replace")
        else:
            info[key] = val
        i += 2

    return info


def _parse_vsearch(raw: Any) -> List[Dict[str, Any]]:
    """Parse GRAPH.VSEARCH response."""
    if not isinstance(raw, list):
        return []

    results: List[Dict[str, Any]] = []
    for item in raw:
        if isinstance(item, list) and len(item) >= 2:
            results.append({
                "node_id": int(item[0]) if not isinstance(item[0], int) else item[0],
                "score": float(_to_str(item[1])) if len(item) > 1 else 0.0,
            })
    return results
