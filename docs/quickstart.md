---
title: "Quick start"
description: "Install, build, and run Moon in under five minutes."
---

# Quick start

## Prerequisites

- [Rust](https://rustup.rs/) stable toolchain (edition 2024)
- cmake (required by aws-lc-rs for TLS support)

## Build from source

### Clone the repository

```bash
git clone https://github.com/pilotspace/moon.git
cd moon
```

### Build the release binary

The default build uses the Monoio runtime with jemalloc:

```bash
cargo build --release
```

??? note "Alternative build configurations"
    ```bash
    # Tokio runtime (required for macOS CI, CodeQL, GitHub Actions)
    cargo build --release --no-default-features --features runtime-tokio

    # Tokio with jemalloc
    cargo build --release --no-default-features --features runtime-tokio,jemalloc

    # Native CPU optimizations (recommended for benchmarking)
    RUSTFLAGS="-C target-cpu=native" cargo build --release
    ```

### Start the server

```bash
# Default: 127.0.0.1:6379, auto-detect CPU count for shards
./target/release/moon

# With specific options
./target/release/moon --port 6379 --shards 4
```

### Connect

Any Redis client works out of the box:

```bash
redis-cli -p 6379
127.0.0.1:6379> PING
PONG
127.0.0.1:6379> SET hello world
OK
127.0.0.1:6379> GET hello
"world"
```

## Docker

```bash
# Build the image
docker build -t moon .

# Run with default settings
docker run -p 6379:6379 moon

# Run with persistence
docker run -p 6379:6379 -v moon-data:/data moon \
  moon --bind 0.0.0.0 --dir /data --appendonly yes

# Run with password and TLS
docker run -p 6379:6379 -p 6380:6380 moon \
  moon --bind 0.0.0.0 --requirepass secret \
  --tls-port 6380 --tls-cert-file /certs/server.crt --tls-key-file /certs/server.key
```

## Client libraries

Moon is compatible with any Redis client. Here are a few examples:

=== "Python"
    ```python
    import redis

    r = redis.Redis(host='localhost', port=6379)
    r.set('key', 'value')
    print(r.get('key'))  # b'value'
    ```

=== "Node.js"
    ```javascript
    import { createClient } from 'redis';

    const client = createClient({ url: 'redis://localhost:6379' });
    await client.connect();
    await client.set('key', 'value');
    console.log(await client.get('key')); // 'value'
    ```

=== "Go"
    ```go
    import "github.com/redis/go-redis/v9"

    rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
    rdb.Set(ctx, "key", "value", 0)
    val, _ := rdb.Get(ctx, "key").Result()
    fmt.Println(val) // "value"
    ```

=== "Java"
    ```java
    Jedis jedis = new Jedis("localhost", 6379);
    jedis.set("key", "value");
    System.out.println(jedis.get("key")); // "value"
    ```

## Python SDK

```bash
pip install moondb
```

```python
from moondb import MoonClient, encode_vector

client = MoonClient(host="localhost", port=6379)

# Standard Redis commands
client.set("hello", "world")

# Vector search
client.vector.create_index("docs", dim=384, metric="COSINE")
client.hset("doc:1", mapping={"vec": encode_vector([0.1] * 384), "title": "Hello"})
results = client.vector.search("docs", [0.1] * 384, k=5)

# Graph engine
client.graph.create("social")
client.graph.add_node("social", "Person", name="Alice")
```

See [sdk/python/README.md](https://github.com/pilotspace/moon/tree/main/sdk/python) for async client, LangChain/LlamaIndex integrations, and full API reference.

## Try Moon-native features

=== "Cross-store transactions"
    ```bash
    redis-cli -p 6379
    127.0.0.1:6379> TXN BEGIN
    OK
    127.0.0.1:6379> SET user:1 alice
    OK
    127.0.0.1:6379> HSET doc:1 title "Hello" vec <vector_bytes>
    (integer) 2
    127.0.0.1:6379> TXN COMMIT
    OK
    ```

=== "Workspaces"
    ```bash
    redis-cli -p 6379
    127.0.0.1:6379> WS CREATE myapp
    "0193a9f2-e456-7890-abcd-ef1234567890"
    127.0.0.1:6379> WS AUTH 0193a9f2-e456-7890-abcd-ef1234567890
    OK
    127.0.0.1:6379> SET user:1 alice
    OK
    127.0.0.1:6379> WS LIST
    1) "0193a9f2-e456-7890-abcd-ef1234567890"
    ```

=== "Message queues"
    ```bash
    redis-cli -p 6379
    127.0.0.1:6379> MQ CREATE orders MAXDELIVERY 5
    OK
    127.0.0.1:6379> MQ PUSH orders action process item_id 42
    "1713394800000-0"
    127.0.0.1:6379> MQ POP orders COUNT 1
    1) 1) "1713394800000-0"
       2) 1) "action" 2) "process" 3) "item_id" 4) "42"
    127.0.0.1:6379> MQ ACK orders 1713394800000-0
    (integer) 1
    ```

=== "Temporal queries"
    ```bash
    redis-cli -p 6379
    127.0.0.1:6379> TEMPORAL.SNAPSHOT_AT
    OK
    127.0.0.1:6379> FT.SEARCH idx "*=>[KNN 5 @v $q]" AS_OF 1713394800000 PARAMS 2 q <vec> DIALECT 2
    ```

## Next steps

<div class="grid cards" markdown>

-   [__Configuration__](configuration.md)
    All CLI flags and options.
-   [__Architecture__](architecture.md)
    How Moon works under the hood.
-   [__Transactions__](guides/transactions.md)
    Cross-store ACID transactions across KV, vector, and graph.
-   [__Full-text search__](guides/full-text-search.md)
    BM25, hybrid fusion, FT.AGGREGATE with GROUPBY/REDUCE.

</div>
