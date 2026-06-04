// -- Global allocator selection -----------------------------------------
// Three states:
//   1. feature = "jemalloc"        -> tikv_jemallocator (production default)
//   2. feature = "mimalloc-alt"    -> mimalloc (opt-in A/B; PERF-11)
//   3. neither                     -> mimalloc (default fallback for builds
//                                    that disable the production allocator)
//
// Enabling BOTH `jemalloc` and `mimalloc-alt` is a compile-time error.

#[cfg(all(feature = "jemalloc", feature = "mimalloc-alt"))]
compile_error!(
    "Features `jemalloc` and `mimalloc-alt` are mutually exclusive. \
     Disable one -- typically: cargo build --no-default-features --features runtime-monoio,mimalloc-alt,graph,text-index"
);

#[cfg(not(feature = "jemalloc"))]
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

#[cfg(feature = "jemalloc")]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

/// `#[repr(transparent)]` wrapper around `*const c_char` so we can declare a
/// `Sync` static that exposes the exact `const char *` ABI jemalloc expects
/// for the `_rjem_malloc_conf` symbol.
#[cfg(feature = "jemalloc")]
#[repr(transparent)]
pub struct MallocConfPtr(*const libc::c_char);

// SAFETY: The pointer is a `'static` C-string literal — immutable for the
// lifetime of the program. jemalloc reads it exactly once during init.
#[cfg(feature = "jemalloc")]
unsafe impl Sync for MallocConfPtr {}

#[cfg(feature = "jemalloc")]
#[allow(non_upper_case_globals)]
#[unsafe(export_name = "_rjem_malloc_conf")]
pub static malloc_conf: MallocConfPtr = MallocConfPtr(
    c"narenas:8,background_thread:true,metadata_thp:auto,dirty_decay_ms:1000,muzzy_decay_ms:5000,abort_conf:true".as_ptr(),
);

use std::path::PathBuf;

use clap::Parser;
use moon::config::ServerConfig;
use moon::persistence::aof::{self, AofMessage, AofWriterPool, FsyncPolicy};
use moon::runtime::cancel::CancellationToken;
use moon::runtime::channel;
use moon::runtime::{RuntimeFactoryImpl, traits::RuntimeFactory};
use moon::server;
use moon::shard::Shard;
use moon::shard::mesh::{CHANNEL_BUFFER_SIZE, ChannelMesh};
use moon::shard::shared_databases::ShardDatabases;
use tracing::info;

fn main() -> anyhow::Result<()> {
    // Re-spawn self with MALLOC_CONF if --memory-arenas-cap differs from the
    // baked-in default (8). Sentinel env var prevents infinite recursion.
    // Must run BEFORE tracing init and clap parse — jemalloc reads MALLOC_CONF
    // at process start, so the env var must be set before exec.
    maybe_respawn_with_arena_override()?;

    // Block SIGTERM in the main thread BEFORE any child threads are spawned
    // (TLS reload thread, Prometheus admin thread, ctrlc internal thread,
    // shard threads). All child threads inherit the blocked mask, so SIGTERM
    // is delivered only to our dedicated `sigterm-handler` thread via sigwait(2).
    // See the "Signal handler setup" block below for the handler threads.
    #[cfg(unix)]
    // SAFETY: sigset_t is a plain C struct zero-initialised to the empty set.
    // sigemptyset, sigaddset, and pthread_sigmask are async-signal-safe POSIX
    // functions. We hold no locks during the mask modification.
    unsafe {
        let mut set: libc::sigset_t = std::mem::zeroed();
        libc::sigemptyset(&mut set);
        libc::sigaddset(&mut set, libc::SIGTERM);
        libc::pthread_sigmask(libc::SIG_BLOCK, &set, std::ptr::null_mut());
    }

    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "moon=info".into()),
        )
        .init();

    let mut config = ServerConfig::parse();

    // ── AOF v1→v2 migration (FIX-W3-2): early-exit before normal boot ──
    // When `--migrate-aof-from` is set, run the migration tool and exit.
    // This must run BEFORE any shard/AOF initialization so the source
    // directory is never modified and the destination is populated atomically.
    if let Some(ref from) = config.migrate_aof_from {
        let to = config.migrate_aof_to.as_deref().ok_or_else(|| {
            anyhow::anyhow!("--migrate-aof-to is required when --migrate-aof-from is set")
        })?;
        if config.migrate_aof_shards == 0 {
            return Err(anyhow::anyhow!(
                "--migrate-aof-shards must be >= 1 when --migrate-aof-from is set"
            ));
        }
        info!(
            "Running AOF migration: {} → {} ({} shards)",
            from.display(),
            to.display(),
            config.migrate_aof_shards
        );
        // Create destination directory if absent.
        if let Err(e) = std::fs::create_dir_all(to) {
            return Err(anyhow::anyhow!(
                "Failed to create migration destination directory {}: {}",
                to.display(),
                e
            ));
        }
        let result =
            moon::persistence::migrate_aof::migrate_aof(from, to, config.migrate_aof_shards)
                .map_err(|e| anyhow::anyhow!("AOF migration failed: {}", e))?;
        info!(
            "AOF migration complete: {} RDB keys migrated, {} commands read, {} written, {} skipped",
            result.rdb_keys_migrated,
            result.commands_read,
            result.commands_written,
            result.commands_skipped
        );
        return Ok(());
    }

    // Non-jemalloc builds: warn if operator explicitly set --memory-arenas-cap
    #[cfg(not(feature = "jemalloc"))]
    if config.memory_arenas_cap != 8 {
        tracing::warn!(
            "--memory-arenas-cap={} is a no-op for non-jemalloc builds",
            config.memory_arenas_cap
        );
    }

    // Protected mode startup warning
    if config.protected_mode == "yes" && config.requirepass.is_none() && config.aclfile.is_none() {
        tracing::warn!(
            "WARNING: no password set. Protected mode is enabled. \
             Only loopback connections are accepted. \
             Use --requirepass or --protected-mode no to change this."
        );
    }

    // G1 memory guardrail: resolve --maxmemory before any RuntimeConfig is
    // built so an unset cap is auto-populated (cgroup-aware) and the startup
    // notice prints exactly once.
    moon::config::log_memory_guardrail(config.apply_memory_guardrail());

    // Build TLS configuration if tls_port is set.
    // Uses ArcSwap for SIGHUP-based certificate hot-reload.
    let tls_config: Option<moon::tls::SharedTlsConfig> = if config.tls_port > 0 {
        let cert = config
            .tls_cert_file
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("--tls-cert-file required when --tls-port is set"))?;
        let key = config
            .tls_key_file
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("--tls-key-file required when --tls-port is set"))?;
        let tls_cfg = moon::tls::build_tls_config(
            cert,
            key,
            config.tls_ca_cert_file.as_deref(),
            config.tls_ciphersuites.as_deref(),
        )
        .map_err(|e| anyhow::anyhow!("Failed to build TLS config: {}", e))?;
        info!(
            "TLS enabled on port {} (TLS 1.3, rustls + aws-lc-rs)",
            config.tls_port
        );
        let shared = moon::tls::make_shared(tls_cfg);

        // Spawn SIGHUP reload thread (Linux only)
        #[cfg(unix)]
        moon::tls::spawn_sighup_reload_thread(
            shared.clone(),
            cert.clone(),
            key.clone(),
            config.tls_ca_cert_file.clone(),
            config.tls_ciphersuites.clone(),
        );

        Some(shared)
    } else {
        None
    };

    // Validate persistence directory is accessible
    if let Err(e) = std::fs::create_dir_all(&config.dir) {
        return Err(anyhow::anyhow!(
            "failed to create persistence directory {:?}: {}",
            config.dir,
            e
        ));
    }

    // --check-config: validate and exit without starting.
    // Runs AFTER TLS cert/key validation, protected mode check, and persistence dir check
    // so that real configuration errors are caught before reporting success.
    // Remaining initialization (metrics, shards, AOF) is runtime-only and not validated here.
    if config.check_config {
        // Validate shard count is reasonable
        if config.shards == 0 {
            return Err(anyhow::anyhow!("--shards must be >= 1"));
        }
        // Validate admin port doesn't conflict with main port
        if config.admin_port > 0 && config.admin_port == config.port {
            return Err(anyhow::anyhow!(
                "--admin-port ({}) must differ from --port ({})",
                config.admin_port,
                config.port
            ));
        }
        if config.admin_port > 0 && config.tls_port > 0 && config.admin_port == config.tls_port {
            return Err(anyhow::anyhow!(
                "--admin-port ({}) must differ from --tls-port ({})",
                config.admin_port,
                config.tls_port
            ));
        }
        if config.tls_port > 0 && config.tls_port == config.port {
            return Err(anyhow::anyhow!(
                "--tls-port ({}) must differ from --port ({})",
                config.tls_port,
                config.port
            ));
        }
        info!("Configuration is valid.");
        return Ok(());
    }

    // ── Admin/console hardening (HARD-01/02/03, Phase 137) ──────────
    // Build the auth + CORS policies BEFORE the admin listener binds so
    // misconfiguration (wildcard CORS + auth required, empty secret) fails
    // fast and never opens a port that would satisfy probes while silently
    // accepting unauthenticated requests.
    //
    // The rate limiter is constructed inside the admin runtime (its
    // cleanup task needs `tokio::spawn`); we thread the raw rps/burst
    // through to `spawn_admin_server`.
    #[cfg(feature = "console")]
    let (console_auth, console_cors) = {
        let auth_policy = if config.console_auth_required {
            let secret = if config.console_auth_secret.is_empty() {
                // Operator did not supply a secret: generate an ephemeral
                // 32-byte secret and warn that issued tokens won't survive
                // restart.
                let bytes: [u8; 32] = rand::random();
                use base64::Engine;
                let s = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(bytes);
                tracing::warn!(
                    "--console-auth-required set without --console-auth-secret; \
                     generated ephemeral secret (tokens will not survive restart). \
                     Set --console-auth-secret=... for reproducible deploys."
                );
                s
            } else {
                config.console_auth_secret.clone()
            };
            match moon::admin::auth::AuthPolicy::enabled(secret.as_bytes()) {
                Ok(p) => std::sync::Arc::new(p),
                Err(e) => return Err(anyhow::anyhow!("--console-auth-secret: {}", e)),
            }
        } else {
            std::sync::Arc::new(moon::admin::auth::AuthPolicy::disabled())
        };

        let cors_policy = match moon::admin::cors::CorsPolicy::new(
            &config.console_cors_origin,
            config.console_auth_required,
        ) {
            Ok(p) => std::sync::Arc::new(p),
            Err(e) => return Err(anyhow::anyhow!(e)),
        };

        (auth_policy, cors_policy)
    };

    // Initialize Prometheus metrics exporter (if admin_port > 0)
    let readiness_flag = moon::admin::metrics_setup::init_metrics(
        config.admin_port,
        &config.bind,
        #[cfg(feature = "console")]
        console_auth,
        #[cfg(feature = "console")]
        console_cors,
        #[cfg(feature = "console")]
        config.console_rate_limit,
        #[cfg(feature = "console")]
        config.console_rate_burst,
    );

    // Initialize global slowlog with user-configured thresholds
    moon::admin::metrics_setup::init_global_slowlog(
        config.slowlog_max_len,
        config.slowlog_log_slower_than,
    );

    // Initialize vector distance dispatch table (must happen before any search).
    moon::vector::distance::init();

    // Determine number of shards
    // T1.2: when --shards 0 (auto-detect), optionally cap at the empirical
    // sweet-spot of min(2, vCPU) via MOON_AUTO_SHARDS_CONSERVATIVE=1.
    // Default behaviour is unchanged: full available_parallelism().
    let num_shards = if config.shards == 0 {
        let parallelism = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(4);
        let conservative = std::env::var_os("MOON_AUTO_SHARDS_CONSERVATIVE").is_some();
        let resolved = compute_auto_shards(parallelism, conservative);
        if conservative {
            info!(
                "auto-detected shards={resolved} \
                 (capped at 2 via MOON_AUTO_SHARDS_CONSERVATIVE; \
                 unset to use full vCPU count of {parallelism})"
            );
        } else {
            info!(
                "auto-detected shards={resolved} \
                 (set MOON_AUTO_SHARDS_CONSERVATIVE=1 to cap at 2)"
            );
        }
        resolved
    } else {
        config.shards
    };

    info!("Starting with {} shards", num_shards);

    // Checked cast: --shards is bounded by clap's value_parser, but `as u16`
    // would silently wrap for values > 65535. Fail loudly instead.
    // ALLOW: panic is appropriate here — this is `main`, not library code.
    #[allow(clippy::expect_used)]
    let shard_count_u16: u16 = u16::try_from(num_shards).expect("--shards must be <= 65535");

    // P0-FIX-01b LIFTED (Option B step 9, 2026-06-01): the per-shard AOF
    // pipeline (RFC steps 1-8) makes `--shards >= 2 + --appendonly yes`
    // crash-safe. CRASH-01-LITE confirms 200/200 keys recover after
    // SIGKILL on a 2-shard everysec config; manual disk inspection shows
    // framed `[u64 lsn LE][u32 len LE][RESP]` entries in each shard's
    // file. The startup refusal is no longer needed.
    //
    // `--unsafe-multishard-aof` is preserved as a no-op flag so existing
    // operator runbooks and CI command lines do not break — the flag
    // emits a one-line info notice if explicitly set, then proceeds as
    // if it were not. Removing the flag entirely is a future cleanup
    // once dependents have been audited.
    if config.unsafe_multishard_aof {
        tracing::warn!(
            "[DEPRECATED] --unsafe-multishard-aof is a no-op and will be removed in v0.2. \
             Per-shard AOF is crash-safe as of PR #129 (CRASH-01-LITE: 200/200). \
             Remove this flag from your launch command or systemd unit."
        );
    }

    // T1.1: warn when maxclients < 25 × shards (undersubscription footgun).
    // Suppressed by MOON_NO_UNDERSUBSCRIPTION_WARN=1.
    if let Some(msg) = should_warn_undersubscription(config.maxclients, num_shards)
        && std::env::var_os("MOON_NO_UNDERSUBSCRIPTION_WARN").is_none()
    {
        tracing::warn!("{msg}");
    }

    // Create channel mesh for inter-shard communication
    let mut mesh = ChannelMesh::new(num_shards, CHANNEL_BUFFER_SIZE);

    // Shared cancellation token for graceful shutdown
    let cancel_token = CancellationToken::new();

    // ── Signal handler setup ──────────────────────────────────────────────────
    //
    // SIGTERM was already blocked at the very top of main() (before any thread
    // is spawned), so all child threads — TLS reload, Prometheus/admin, ctrlc's
    // internal thread, shard threads — inherit the blocked mask. The kernel
    // therefore cannot deliver SIGTERM to any of those threads; it queues until
    // our dedicated `sigterm-handler` thread below consumes it via sigwait(2).
    //
    // Steps here:
    // 1. Install the SIGINT/Ctrl+C handler (ctrlc spawns its thread here; it
    //    inherits the already-blocked SIGTERM mask).
    // 2. Spawn the dedicated SIGTERM handler thread (also inherits the mask).

    // Step 1: Centralized SIGINT/Ctrl+C handler.
    //
    // Installed here in main so BOTH runtimes share a single handler.
    // The listener no longer installs its own ctrl_c handler; it only polls
    // `shutdown.cancelled()`.
    {
        let sigint_token = cancel_token.clone();
        ctrlc::set_handler(move || {
            info!("Shutdown signal received");
            sigint_token.cancel();
        })
        .map_err(|e| anyhow::anyhow!("failed to set Ctrl+C handler: {e}"))?;
    }

    // Step 2: SIGTERM graceful shutdown handler (Unix only).
    //
    // ctrlc (without the `termination` feature) handles SIGINT/Ctrl+C only.
    // systemd/launchd send SIGTERM on `systemctl stop` / `launchctl stop`,
    // which was previously unhandled — the kernel delivered the default action
    // (non-zero exit, AOF not flushed).
    //
    // We use a dedicated OS thread with sigwait(2) so that:
    //   1. SIGHUP is NOT touched (avoids clobbering the TLS cert-reload handler).
    //   2. The handler works on both `runtime-tokio` and `runtime-monoio`.
    //   3. Signal safety: cancel() is backed by atomics — safe from any context.
    //
    // SIGTERM was blocked at the top of main() before any threads were spawned,
    // so every thread (including ctrlc's thread and all shard threads) has it
    // blocked. The sigterm-handler thread keeps SIGTERM blocked and consumes it
    // via sigwait.
    #[cfg(unix)]
    {
        let sigterm_token = cancel_token.clone();
        std::thread::Builder::new()
            .name("sigterm-handler".to_string())
            .spawn(move || {
                // SIGTERM is already blocked in this thread (inherited from main
                // before any threads spawned) — the precondition for sigwait(2),
                // which atomically consumes a pending blocked signal.
                // SAFETY: sigset_t is a plain C struct zero-initialised to the
                // empty set; sigemptyset/sigaddset/sigwait are async-signal-safe
                // POSIX functions; the set is exclusively owned by this thread.
                unsafe {
                    let mut set: libc::sigset_t = std::mem::zeroed();
                    libc::sigemptyset(&mut set);
                    libc::sigaddset(&mut set, libc::SIGTERM);
                    loop {
                        let mut sig: libc::c_int = 0;
                        let ret = libc::sigwait(&set, &mut sig);
                        if ret != 0 {
                            tracing::error!(
                                "sigwait(SIGTERM) failed: {}",
                                std::io::Error::from_raw_os_error(ret)
                            );
                            continue;
                        }
                        if sig == libc::SIGTERM {
                            info!("Shutdown signal received");
                            sigterm_token.cancel();
                            return;
                        }
                    }
                }
            })
            .map_err(|e| anyhow::anyhow!("failed to spawn SIGTERM handler thread: {e}"))?;
    }

    // Collect connection senders for the listener before spawning shard threads
    let conn_txs: Vec<_> = (0..num_shards).map(|i| mesh.conn_tx(i)).collect();

    // Set up AOF writer channel(s) + `AofWriterPool` (step 2f-β: layout-aware).
    //
    // Logic:
    //   1. If `appendonly == "yes"` and an existing on-disk manifest is found,
    //      verify its shard count matches `--shards` (RFC § 3 refusal — a
    //      mismatch silently maps shards to the wrong AOF files and is fatal).
    //   2. If the manifest's layout is `PerShard` AND `num_shards >= 2`,
    //      spawn one writer per shard (`aof-writer-{N}` threads) and emit a
    //      `AofWriterPool::per_shard(senders)`.
    //   3. Otherwise spawn the single legacy writer and emit
    //      `AofWriterPool::top_level(tx)`. This includes:
    //        - no manifest yet (fresh install — `initialize()` only writes
    //          TopLevel today; fresh-install PerShard creation lands later
    //          in the RFC sequence)
    //        - existing TopLevel manifest (legacy v1 or single-shard v2)
    //        - `num_shards == 1` (always TopLevel; per-shard fan-out has no
    //          meaning when there is one shard)
    //
    // A *corrupt* manifest is fatal — `AofManifest::load` returning `Err(_)`
    // must NOT silently fall back to TopLevel, because the next write would
    // create a fresh manifest overwriting the reference to the real base RDB
    // and lose data. This mirrors the replay block at L514–526.
    //
    // Note: nothing today constructs a `layout == PerShard` manifest on disk
    // (initialize() hardcodes TopLevel, migrate_top_level_to_per_shard is not
    // yet wired into boot). The PerShard branch is reachable only by a
    // hand-crafted manifest until step 9 lifts the multi-shard gate. Runtime
    // behavior under default configurations stays byte-identical to step 2f-α.
    use moon::persistence::aof_manifest::{AofLayout, AofManifest};
    let existing_manifest: Option<AofManifest> = if config.appendonly == "yes" {
        let base_dir = PathBuf::from(&config.dir);
        match AofManifest::load(&base_dir) {
            Ok(opt) => opt,
            Err(e) => {
                eprintln!(
                    "REFUSING TO START: AOF manifest at {}/appendonlydir/ is corrupt: {}. \
                     Inspect manually before deleting; overwriting silently loses data.",
                    base_dir.display(),
                    e
                );
                std::process::exit(2);
            }
        }
    } else {
        None
    };
    // TopLevel + multi-shard refusal — hoisted here so it fires on both
    // runtime-monoio and runtime-tokio. The TopLevel manifest (v1, shards=1)
    // combined with --shards >= 2 silently loses data for shards 1..N because
    // the single shared AOF replays everything into shard 0 while shards 1..N
    // start empty. This check fires unconditionally on both runtimes; the
    // duplicate check inside the runtime-monoio recovery block (further below)
    // is now dead code but harmless — it guards operator data against future
    // code paths that bypass this early exit.
    if let Some(ref m) = existing_manifest
        && m.layout == AofLayout::TopLevel
        && num_shards >= 2
    {
        let aof_base = std::path::Path::new(&config.dir).join("appendonlydir");
        let manifest_path = aof_base.join("moon.aof.manifest");
        let num_shards_minus_one = num_shards - 1;
        eprintln!(
            "REFUSING TO START: legacy TopLevel AOF manifest at {manifest_path} \
             detected with --shards {num_shards} (>= 2). \
             This combination silently loses data for shards 1..={num_shards_minus_one}. \
             To migrate: stop the server, remove {aof_dir}, then restart with \
             --shards {num_shards} --appendonly yes (Moon creates a fresh per-shard \
             manifest; load prior state from dump.rdb first if needed). \
             See docs/runbooks/multi-shard-aof-rewrite.md for full migration instructions.",
            manifest_path = manifest_path.display(),
            num_shards = num_shards,
            num_shards_minus_one = num_shards_minus_one,
            aof_dir = aof_base.display(),
        );
        std::process::exit(2);
    }
    // Shard-count mismatch guard for non-TopLevel manifests (PerShard layout
    // with a different shard count than currently configured). A v1 TopLevel
    // manifest always records shards=1; that case is already handled above.
    if let Some(ref m) = existing_manifest
        && let Err(e) = m.verify_shard_count(shard_count_u16)
    {
        eprintln!("REFUSING TO START: {e}");
        std::process::exit(2);
    }

    let aof_pool: Option<std::sync::Arc<AofWriterPool>> = if config.appendonly == "yes" {
        let fsync = FsyncPolicy::from_str(&config.appendfsync);
        // PerShard writers required when num_shards >= 2 AND we'll have a
        // PerShard manifest at runtime. Two cases produce PerShard:
        //   1. existing manifest is already PerShard, OR
        //   2. no manifest yet (first boot) — main.rs will call
        //      `initialize_multi(num_shards)` later in the recovery block.
        // The legacy case (existing TopLevel manifest on a multi-shard
        // deployment) sticks with the TopLevel writer pending the migrate-aof
        // tool — the multi-shard replay branch already warns about this.
        let multi_shard_no_manifest = existing_manifest.is_none() && num_shards >= 2;
        let use_per_shard = num_shards >= 2
            && (matches!(
                existing_manifest.as_ref().map(|m| m.layout),
                Some(AofLayout::PerShard)
            ) || multi_shard_no_manifest);

        if use_per_shard {
            let base_dir = PathBuf::from(&config.dir);
            let mut senders = Vec::with_capacity(num_shards);
            for sid in 0..num_shards {
                let (tx, rx) = channel::mpsc_bounded::<AofMessage>(10_000);
                let aof_token = cancel_token.child_token();
                let base_dir = base_dir.clone();
                let thread_name = format!("aof-writer-{sid}");
                let thread_name_inner = thread_name.clone();
                std::thread::Builder::new()
                    .name(thread_name)
                    .spawn(move || {
                        RuntimeFactoryImpl::block_on_local(
                            thread_name_inner,
                            aof::per_shard_aof_writer_task(
                                rx, base_dir, sid as u16, fsync, aof_token,
                            ),
                        );
                    })
                    .expect("failed to spawn per-shard AOF writer thread");
                senders.push(tx);
            }
            info!(
                "AOF enabled (PerShard, {} writers, fsync: {:?})",
                num_shards, fsync
            );
            // [F6] per_shard_with_base_dir records the persistence base dir so a
            // per-shard BGREWRITEAOF can load the authoritative manifest fresh
            // at rewrite time (try_send_rewrite_per_shard).
            Some(AofWriterPool::per_shard_with_base_dir(
                senders,
                fsync,
                std::time::Duration::from_millis(config.aof_fsync_timeout_ms),
                base_dir.clone(),
            ))
        } else {
            let (tx, rx) = channel::mpsc_bounded::<AofMessage>(10_000);
            let aof_token = cancel_token.child_token();
            let aof_file_path = PathBuf::from(&config.dir).join(&config.appendfilename);
            // Legacy single-writer thread. Each shard clones the outer
            // `aof_pool` Arc; sender lifetime is governed by the pool's Drop.
            std::thread::Builder::new()
                .name("aof-writer".to_string())
                .spawn(move || {
                    RuntimeFactoryImpl::block_on_local(
                        "aof-writer".to_string(),
                        aof::aof_writer_task(rx, aof_file_path, fsync, aof_token),
                    );
                })
                .expect("failed to spawn AOF writer thread");
            info!("AOF enabled (TopLevel, fsync: {:?})", fsync);
            Some(AofWriterPool::top_level_with_policy(
                tx,
                fsync,
                std::time::Duration::from_millis(config.aof_fsync_timeout_ms),
            ))
        }
    } else {
        None
    };

    // Compute bind address for SO_REUSEPORT per-shard listeners (Linux io_uring path).
    let bind_addr = format!("{}:{}", config.bind, config.port);

    // FIX-W1-4: gate BGREWRITEAOF whenever per-shard AOF is active
    // (num_shards >= 2 + appendonly=yes). The original gate was too narrow:
    // it required disk_offload to be enabled, missing the plain AOF case.
    // Per-shard rewrite is not yet implemented (AofPoolSendError::
    // RewriteUnsupportedInPerShard); the pool already refuses the message,
    // but this early gate provides a stable, documented error to operators
    // BEFORE the channel send so no in-progress flag is flipped.
    // Verified 2026-05-26: multi-shard BGREWRITEAOF loses ~38% of keys on
    // restart. Gate lifted only when multi-part AOF replay ships (v2.0+).
    // See docs/runbooks/multi-shard-aof-rewrite.md.
    // [F6] When `--experimental-per-shard-rewrite` is set, leave the gate OPEN
    // so BGREWRITEAOF routes to the per-shard fan-out coordinator
    // (try_send_rewrite_per_shard): synchronized seq bump + single manifest
    // commit across all per-shard writers, validated by
    // tests/crash_matrix_per_shard_bgrewriteaof.rs. Default (flag off) keeps
    // the gate closed — the shipped, crash-safe "no in-place compaction"
    // behavior that avoided the historical ~38%-key-loss-on-restart.
    if config.per_shard_aof_active(num_shards) {
        if config.experimental_per_shard_rewrite {
            tracing::warn!(
                shards = num_shards,
                appendonly = %config.appendonly,
                "BGREWRITEAOF per-shard rewrite ENABLED (--experimental-per-shard-rewrite). \
                 Per-shard fan-out compaction is active; this path is experimental."
            );
        } else {
            moon::command::persistence::MULTI_SHARD_AOF_REWRITE_UNSAFE
                .store(true, std::sync::atomic::Ordering::Relaxed);
            tracing::warn!(
                shards = num_shards,
                appendonly = %config.appendonly,
                "BGREWRITEAOF gated: per-shard AOF layout active (see docs/runbooks/multi-shard-aof-rewrite.md). Use --shards 1, or --experimental-per-shard-rewrite to enable per-shard compaction."
            );
        }
    }

    // Create watch channel for snapshot triggers (auto-save and BGSAVE)
    let (snapshot_trigger_tx, snapshot_trigger_rx) = moon::runtime::channel::watch(0u64);

    // Persistence directory for per-shard WAL and snapshots.
    // Only set when persistence is actually enabled (appendonly=yes or save rules exist)
    // to avoid creating WAL writers that fsync on every tick for no benefit.
    let persistence_dir = if config.appendonly == "yes" || config.save.is_some() {
        Some(config.dir.clone())
    } else {
        None
    };

    // Create replication state -- load persisted repl_id or generate new one.
    let (repl_id, repl_id2) =
        moon::replication::state::load_replication_state(std::path::Path::new(&config.dir));
    let repl_state = std::sync::Arc::new(std::sync::RwLock::new(
        moon::replication::state::ReplicationState::new(num_shards, repl_id, repl_id2),
    ));

    // Register repl_state globally for INFO command queries.
    moon::admin::metrics_setup::set_global_repl_state(repl_state.clone());

    // Cluster mode initialization
    let cluster_state: Option<std::sync::Arc<std::sync::RwLock<moon::cluster::ClusterState>>> =
        if config.cluster_enabled {
            moon::cluster::CLUSTER_ENABLED.store(true, std::sync::atomic::Ordering::Relaxed);
            let self_addr: std::net::SocketAddr = format!("{}:{}", config.bind, config.port)
                .parse()
                .expect("invalid bind address");
            let node_id = moon::replication::state::generate_repl_id();
            let state = moon::cluster::ClusterState::new(node_id, self_addr);
            let cs = std::sync::Arc::new(std::sync::RwLock::new(state));
            info!(
                "Cluster mode enabled, node ID: {}",
                cs.read().unwrap().node_id
            );
            Some(cs)
        } else {
            None
        };

    // Build ACL table from config (load aclfile if configured, else bootstrap from requirepass)
    let acl_table: std::sync::Arc<std::sync::RwLock<moon::acl::AclTable>> = {
        let table = moon::acl::AclTable::load_or_default(&config);
        std::sync::Arc::new(std::sync::RwLock::new(table))
    };

    // Build shared runtime config for sharded handlers
    let runtime_config_shared: std::sync::Arc<parking_lot::RwLock<moon::config::RuntimeConfig>> =
        { std::sync::Arc::new(parking_lot::RwLock::new(config.to_runtime_config())) };
    // Publish the resolved shard count so eviction enforces maxmemory as a
    // whole-instance cap (per-shard budget = maxmemory / num_shards). Without
    // this, each shard would tolerate the full maxmemory → ~N× aggregate RSS.
    runtime_config_shared.write().num_shards = num_shards;
    moon::config::log_maxmemory_sharding(runtime_config_shared.read().maxmemory, num_shards);
    let server_config_shared: std::sync::Arc<moon::config::ServerConfig> =
        { std::sync::Arc::new(config.clone()) };

    // MA12: Initialise disk free-space monitor.
    // Monitors the WAL/persistence volume. When disk_free_min_pct == 0, the
    // monitor is inactive (poll_global is a no-op, is_write_paused always false).
    {
        let monitor_path = persistence_dir.as_deref().unwrap_or(&config.dir);
        moon::shard::disk_monitor::init_global(config.disk_free_min_pct, monitor_path);
    }

    // Collect all notifiers before spawning shard threads
    let all_notifiers = mesh.all_notifiers();

    // Create admin SPSC channels for the console gateway (one per shard).
    #[cfg(feature = "console")]
    let mut admin_consumers = {
        let (admin_producers, admin_consumers) =
            moon::shard::mesh::create_admin_channels(num_shards, CHANNEL_BUFFER_SIZE);
        let gateway = std::sync::Arc::new(moon::admin::console_gateway::ConsoleGateway::new(
            admin_producers,
            all_notifiers.clone(),
        ));
        if moon::admin::console_gateway::set_global_gateway(gateway).is_err() {
            panic!("console gateway initialized twice — bootstrap bug");
        }
        tracing::info!(
            "Console gateway initialized with {} shard channels",
            num_shards
        );
        admin_consumers
    };

    // Pre-create shared pubsub registries for cross-shard introspection reads.
    let all_pubsub_registries: Vec<
        std::sync::Arc<parking_lot::RwLock<moon::pubsub::PubSubRegistry>>,
    > = (0..num_shards)
        .map(|_| std::sync::Arc::new(parking_lot::RwLock::new(moon::pubsub::PubSubRegistry::new())))
        .collect();

    // Pre-create shared remote subscriber maps for zero-SPSC subscription propagation.
    let all_remote_sub_maps: Vec<
        std::sync::Arc<
            parking_lot::RwLock<moon::shard::remote_subscriber_map::RemoteSubscriberMap>,
        >,
    > = (0..num_shards)
        .map(|_| {
            std::sync::Arc::new(parking_lot::RwLock::new(
                moon::shard::remote_subscriber_map::RemoteSubscriberMap::new(),
            ))
        })
        .collect();

    // Create shared affinity tracker for pub/sub connection routing
    let affinity_tracker = std::sync::Arc::new(parking_lot::RwLock::new(
        moon::shard::affinity::AffinityTracker::new(),
    ));

    // Create and restore all shards on main thread, then extract databases
    // into centralized ShardDatabases for cross-shard direct read access.
    let disk_offload_base = if config.disk_offload_enabled() {
        Some(config.effective_disk_offload_dir())
    } else {
        None
    };
    let mut shards: Vec<Shard> = (0..num_shards)
        .map(|id| {
            let mut shard = Shard::with_initial_keyspace_hint(
                id,
                num_shards,
                config.databases,
                config.initial_keyspace_hint,
                config.to_runtime_config(),
            );
            // Recover whenever there is something to recover. Disk-offload cold
            // recovery (v3: heap reload + rebuild_from_manifest) is INDEPENDENT of
            // AOF, but `persistence_dir` is intentionally None under appendonly=no
            // (to avoid per-tick WAL fsync writers). Gating recovery on it alone
            // silently dropped ALL cold data on restart under --appendonly no +
            // disk-offload (cold read-through 0/200; "v3 recovery complete" never
            // logged). Fire recovery when an offload base exists too; the v3 path
            // reads the offload manifest, and the dir arg is used only by the v2
            // fallback (a no-op when no appendonly.aof/snapshot exists).
            if persistence_dir.is_some() || disk_offload_base.is_some() {
                let recover_dir = persistence_dir.as_deref().unwrap_or(config.dir.as_str());
                shard.restore_from_persistence(recover_dir, disk_offload_base.as_deref());
            }
            // Initialize cold_index + cold_shard_dir for disk offload
            if let Some(ref offload_base) = disk_offload_base {
                let shard_dir = offload_base.join(format!("shard-{}", id));
                for db in &mut shard.databases {
                    db.cold_shard_dir = Some(shard_dir.clone());
                    if db.cold_index.is_none() {
                        db.cold_index = Some(moon::storage::tiered::cold_index::ColdIndex::new());
                    }
                }
            }
            shard
        })
        .collect();

    // Multi-part AOF replay/init layered on top of v2/v3 recovery.
    // Priority: if appendonlydir/ manifest exists → load multi-part (skip legacy v2 fallback).
    // Otherwise v2 already handled legacy appendonly.aof during restore_from_persistence.
    //
    // A corrupt manifest is FATAL on BOTH runtimes (previously warn+continue
    // under tokio): overwriting it silently destroys the reference to the real
    // base RDB and loses all persisted data.
    //
    // Runtime split (the regression boundary): the multi-shard PerShard paths
    // (initialize_multi + replay_per_shard) run under BOTH runtimes — the tokio
    // per_shard_aof_writer_task writes the identical framed incr format
    // (`[u64 lsn LE][u32 len LE][RESP]`) and replay_per_shard is
    // runtime-agnostic. The SINGLE-shard multi-part paths stay monoio-only:
    // tokio --shards 1 uses legacy single-file appendonly.aof (v2) + in-place
    // BGREWRITEAOF, so engaging multi-part replay there finds an empty manifest,
    // no rewritten base RDB, and wipes the v2-loaded state — the
    // test_txn_commit_wal_crash_recovery regression. Each single-shard branch
    // below is therefore #[cfg(runtime-monoio)] with a tokio warn fallback.
    //
    // ── B-2: preserve cold-tier wiring across AOF/RDB recovery ──────────────
    // The multi-part AOF replay below invokes `rdb::load`, which loads the base
    // RDB into fresh `Database::new()` temporaries and swaps them wholesale into
    // the live databases (`*live = temp`). That swap silently drops
    // `cold_shard_dir` and the rebuilt `cold_index`: both are live-tier topology,
    // NOT part of the RDB hot snapshot, so a 0-key base RDB still wipes them.
    // Capture them here — after `restore_from_persistence` rebuilt the index and
    // the map wired the dir, before replay clobbers — and re-attach after the
    // replay block so disk-offload read-through survives a restart.
    //
    // The fix lives here (recovery path), NOT in the generic `rdb::load`, on
    // purpose: `rdb::load` also serves replica full-sync and DEBUG RELOAD, which
    // load a *foreign* dataset whose values do NOT live in this node's cold
    // files — preserving the local index there would surface stale reads. Here
    // the loaded base + replayed incrs are this node's own data, so the rebuilt
    // index is authoritative. Pairs with the spill file_id seed
    // (eviction.rs::next_spill_file_id_seed): the seed keeps recovered cold
    // files immutable so these preserved entries stay valid until the
    // steady-state cascade refreshes them.
    let preserved_cold_wiring: Vec<
        Vec<(
            Option<std::path::PathBuf>,
            Option<moon::storage::tiered::cold_index::ColdIndex>,
        )>,
    > = if disk_offload_base.is_some() {
        shards
            .iter_mut()
            .map(|s| {
                s.databases
                    .iter_mut()
                    .map(|db| (db.cold_shard_dir.take(), db.cold_index.take()))
                    .collect()
            })
            .collect()
    } else {
        Vec::new()
    };
    if config.appendonly == "yes"
        && let Some(ref dir) = persistence_dir
    {
        use anyhow::Context;
        use moon::persistence::aof_manifest::AofManifest;
        use moon::persistence::replay::DispatchReplayEngine;
        let base_dir = std::path::PathBuf::from(dir);
        let manifest_opt = AofManifest::load(&base_dir).with_context(|| {
                format!(
                    "AOF manifest at {}/appendonlydir/ is corrupt; refusing to start to avoid data loss. Inspect manually before deleting.",
                    base_dir.display()
                )
            })?;
        if let Some(ref manifest) = manifest_opt {
            if num_shards == 1 {
                // Single-shard multi-part replay is monoio-only (the regression
                // boundary). Under tokio, --shards 1 uses legacy v2 single-file
                // recovery; engaging multi-part replay here wipes v2 state.
                #[cfg(feature = "runtime-monoio")]
                {
                    // Multi-part AOF is authoritative. Wipe any state that earlier
                    // recovery phases (per-shard WAL replay, legacy appendonly.aof
                    // fallback inside restore_from_persistence) may have loaded —
                    // otherwise non-idempotent commands from the incr log would
                    // double-apply on top of that pre-existing state.
                    for db in shards[0].databases.iter_mut() {
                        db.clear();
                    }
                    let loaded = moon::persistence::aof_manifest::replay_multi_part(
                        &mut shards[0].databases,
                        manifest,
                        &DispatchReplayEngine::new(),
                    )
                    .with_context(|| "multi-part AOF replay failed")?;
                    info!(
                        "AOF multi-part loaded (seq {}): {} entries",
                        manifest.seq, loaded
                    );

                    // Retire legacy appendonly.aof so future boots don't double-
                    // replay it via restore_from_persistence's fallback path.
                    // Rename (not delete) so an operator can recover if something
                    // went wrong.
                    let legacy = base_dir.join("appendonly.aof");
                    if legacy.exists() {
                        let retired = base_dir.join("appendonly.aof.legacy");
                        if let Err(e) = std::fs::rename(&legacy, &retired) {
                            tracing::warn!(
                                "Failed to retire legacy AOF {}: {}",
                                legacy.display(),
                                e
                            );
                        } else {
                            info!(
                                "Retired legacy AOF {} → {}",
                                legacy.display(),
                                retired.display()
                            );
                        }
                    }
                }
                #[cfg(not(feature = "runtime-monoio"))]
                {
                    // tokio + --shards 1: single-shard multi-part replay is
                    // monoio-only. Legacy v2 (appendonly.aof) recovery already
                    // ran in restore_from_persistence; warn so an operator who
                    // switched from monoio knows multi-part data isn't loaded by
                    // this build.
                    tracing::warn!(
                        "multi-part AOF manifest at {}/appendonlydir/ found but runtime is \
                         tokio with --shards 1; single-shard multi-part replay is monoio-only. \
                         Legacy v2 (appendonly.aof) recovery active. Switch to monoio to load \
                         multi-part single-shard data.",
                        base_dir.display()
                    );
                }
            } else if manifest.layout == moon::persistence::aof_manifest::AofLayout::PerShard {
                // Per-shard AOF replay (RFC § 2 rules 1-3, Option B step 4).
                //
                // Wipe any state earlier recovery phases loaded for each shard —
                // base RDB + framed incr together are authoritative for that
                // shard, and non-idempotent commands in the incr stream would
                // otherwise double-apply on top of WAL/legacy state.
                for shard in shards.iter_mut() {
                    for db in shard.databases.iter_mut() {
                        db.clear();
                    }
                }

                // Borrow each shard's `databases` mutably and route through
                // `replay_per_shard`. The split_at_mut walk constructs a
                // Vec<&mut [Database]> without aliasing, which `replay_per_shard`
                // requires.
                //
                // `replay_per_shard` now spawns one thread per shard via
                // `std::thread::scope`. The factory closure produces an independent
                // `DispatchReplayEngine` per thread, avoiding the `!Sync` `RefCell`
                // conflict that would arise from sharing a single engine instance
                // across threads (under the `graph` feature).
                let engine_factory =
                    || -> Box<dyn moon::persistence::replay::CommandReplayEngine + Send> {
                        Box::new(DispatchReplayEngine::new())
                    };
                let (total, global_max_lsn, ordered_entries) = {
                    let mut slices: Vec<&mut [moon::storage::Database]> =
                        Vec::with_capacity(shards.len());
                    let mut rest: &mut [moon::shard::Shard] = &mut shards[..];
                    while let Some((head, tail)) = rest.split_first_mut() {
                        slices.push(&mut head.databases);
                        rest = tail;
                    }
                    moon::persistence::aof_manifest::replay_per_shard(
                        &mut slices,
                        manifest,
                        &engine_factory,
                    )
                    .with_context(|| "per-shard AOF replay failed")?
                };

                // Step 5: merge-replay `OrderedAcrossShards`-tagged entries
                // in global LSN order. Today this list is always empty
                // (no production emitter); the path exists so the future
                // cross-shard TXN consumer wires in without a recovery
                // re-design.
                let ordered_engine = DispatchReplayEngine::new();
                let ordered_count = if !ordered_entries.is_empty() {
                    let mut slices: Vec<&mut [moon::storage::Database]> =
                        Vec::with_capacity(shards.len());
                    let mut rest: &mut [moon::shard::Shard] = &mut shards[..];
                    while let Some((head, tail)) = rest.split_first_mut() {
                        slices.push(&mut head.databases);
                        rest = tail;
                    }
                    moon::persistence::aof_manifest::replay_ordered_merge(
                        &mut slices,
                        ordered_entries,
                        &ordered_engine,
                    )
                    .with_context(|| "per-shard AOF ordered merge replay failed")?
                } else {
                    0
                };

                info!(
                    "AOF per-shard loaded (seq {}): {} entries across {} shards (global max lsn {}, ordered merge {} entries)",
                    manifest.seq,
                    total,
                    manifest.shards.len(),
                    global_max_lsn,
                    ordered_count
                );

                // RFC § 2 Rule 3 — seed master_repl_offset before accepting
                // client traffic so the next write doesn't reissue an LSN
                // already on disk.
                if global_max_lsn > 0
                    && let Ok(state) = repl_state.read()
                {
                    state.seed_master_offset(global_max_lsn);
                }

                // Retire any stray legacy top-level appendonly.aof so the
                // next boot doesn't double-replay it via v2 recovery in
                // `restore_from_persistence`.
                let legacy = base_dir.join("appendonly.aof");
                if legacy.exists() {
                    let retired = base_dir.join("appendonly.aof.legacy");
                    if let Err(e) = std::fs::rename(&legacy, &retired) {
                        tracing::warn!("Failed to retire legacy AOF {}: {}", legacy.display(), e);
                    } else {
                        info!(
                            "Retired legacy AOF {} → {}",
                            legacy.display(),
                            retired.display()
                        );
                    }
                }
            } else {
                // TopLevel manifest (v1 / single-file layout) combined with
                // --shards >= 2 is an unsafe combination: replaying a single
                // shared AOF file into multiple shards would assign all data
                // to shard 0 while shards 1..N start empty. This silently
                // loses data that was written to shards 1..N before the
                // manifest was last updated.
                //
                // Previously a warn! + continue, which allowed the server to
                // boot with an empty AOF state. Now a hard refusal so the
                // operator is forced to migrate before proceeding.
                eprintln!(
                    "REFUSING TO START: legacy TopLevel AOF manifest at {manifest_path} \
                     detected with --shards {num_shards} (>= 2). \
                     This combination silently loses data for shards 1..={num_shards_minus_one}. \
                     To migrate: stop the server, remove {aof_dir}, then restart with \
                     --shards {num_shards} --appendonly yes (Moon creates a fresh per-shard \
                     manifest; load prior state from dump.rdb first if needed). \
                     See docs/runbooks/multi-shard-aof-rewrite.md for full migration instructions.",
                    manifest_path = base_dir
                        .join("appendonlydir")
                        .join("moon.aof.manifest")
                        .display(),
                    num_shards = num_shards,
                    num_shards_minus_one = num_shards - 1,
                    aof_dir = base_dir.join("appendonlydir").display(),
                );
                std::process::exit(2);
            }
        } else {
            // No manifest present — first boot after upgrade from legacy
            // single-file AOF (v2 recovery already loaded it above) or
            // fresh install.
            //
            // If restore_from_persistence loaded any state (from WAL or
            // legacy appendonly.aof), we MUST capture it as the seq 1
            // base RDB. Otherwise on the next boot the multi-part replay
            // path would clear the databases and lose the legacy state.
            // Only shard 0 is relevant in single-shard mode (the only
            // mode the multi-part path currently supports).
            let has_state = num_shards == 1 && shards[0].databases.iter().any(|db| db.len() > 0);
            if has_state {
                // Single-shard legacy-upgrade capture is monoio-only. Under
                // tokio, --shards 1 keeps v2 single-file recovery (no manifest);
                // creating a seq-1 base here would trigger the empty-manifest
                // replay regression on the next boot.
                #[cfg(feature = "runtime-monoio")]
                {
                    let rdb_bytes = moon::persistence::rdb::save_to_bytes(&shards[0].databases)
                        .with_context(|| "failed to serialize legacy state for AOF base")?;
                    AofManifest::initialize_with_base(&base_dir, &rdb_bytes)
                        .with_context(|| "failed to initialize AOF manifest with base")?;
                    info!(
                        "First-upgrade: captured legacy state as AOF base seq 1 ({} bytes)",
                        rdb_bytes.len()
                    );
                    // Retire legacy appendonly.aof — its contents are now in
                    // the base RDB, and leaving it would cause v2 recovery on
                    // the next boot to double-replay it.
                    let legacy = base_dir.join("appendonly.aof");
                    if legacy.exists() {
                        let retired = base_dir.join("appendonly.aof.legacy");
                        if let Err(e) = std::fs::rename(&legacy, &retired) {
                            tracing::warn!(
                                "Failed to retire legacy AOF {}: {}",
                                legacy.display(),
                                e
                            );
                        }
                    }
                }
            } else if num_shards >= 2 {
                // Multi-shard fresh boot: create the PerShard manifest layout
                // (RFC § 3) instead of the legacy single-file TopLevel layout.
                // Step 2f-β's spawn-site gate only enables PerShard writers
                // when the loaded manifest's layout is PerShard, so without
                // this branch a multi-shard --appendonly yes deployment would
                // silently fall back to TopLevel and lose data on restart.
                AofManifest::initialize_multi(&base_dir, shard_count_u16)
                    .with_context(|| "failed to initialize PerShard AOF manifest")?;
                info!(
                    "Initialized PerShard AOF manifest for {} shards at {}",
                    num_shards,
                    base_dir.display()
                );
            } else {
                // Single-shard fresh boot.
                #[cfg(feature = "runtime-monoio")]
                AofManifest::initialize(&base_dir)
                    .with_context(|| "failed to initialize AOF manifest")?;
                // tokio --shards 1 fresh: no manifest (v2 single-file recovery
                // owns single-shard durability). Creating one here would trigger
                // the empty-manifest replay regression.
            }
        }
    }

    // (The former standalone tokio "multi-part AOF ignored" warn block was
    // removed: multi-shard PerShard AOF is now loaded on tokio too, and the
    // single-shard tokio warn is emitted inline above.)

    // ── B-2: re-attach cold-tier wiring dropped by the rdb::load swap ───────
    // Restore the `cold_shard_dir` + rebuilt `cold_index` captured before the
    // AOF replay block. Without this, the handler reads a database with
    // `cold_shard_dir = None`, so every read-through of a re-evicted key misses
    // and disk-offload silently loses data across a restart.
    if !preserved_cold_wiring.is_empty() {
        for (shard, dbs) in shards.iter_mut().zip(preserved_cold_wiring) {
            for (db, (cold_shard_dir, cold_index)) in shard.databases.iter_mut().zip(dbs) {
                db.cold_shard_dir = cold_shard_dir;
                db.cold_index = cold_index;
            }
        }
    }

    // Extract databases from all shards and wrap in ShardDatabases
    let all_dbs: Vec<Vec<moon::storage::Database>> = shards
        .iter_mut()
        .map(|s| std::mem::take(&mut s.databases))
        .collect();
    let shard_databases = ShardDatabases::new(all_dbs);

    // Recover graph stores from persistence (CSR segments + metadata + WAL replay).
    #[cfg(feature = "graph")]
    if let Some(ref dir) = persistence_dir {
        let dir_path = std::path::Path::new(dir);
        shard_databases.recover_graph_stores(dir_path);
        shard_databases.replay_graph_wal(dir_path);
    }

    // Replay temporal WAL records (not gated on graph feature — temporal KV is core).
    if let Some(ref dir) = persistence_dir {
        let dir_path = std::path::Path::new(dir);
        shard_databases.replay_temporal_wal(dir_path);
    }

    // Replay workspace WAL records (not gated on graph feature — workspaces are core).
    if let Some(ref dir) = persistence_dir {
        let dir_path = std::path::Path::new(dir);
        shard_databases.replay_workspace_wal(dir_path);
    }

    // Replay MQ WAL records (cursor-rollback for durable queues).
    if let Some(ref dir) = persistence_dir {
        let dir_path = std::path::Path::new(dir);
        shard_databases.replay_mq_wal(dir_path);
    }

    // All shards recovered — mark server as ready for /readyz.
    moon::admin::metrics_setup::set_server_ready();
    // Register global ShardDatabases for MEMORY DOCTOR + Prometheus per-kind gauges.
    moon::admin::metrics_setup::set_global_shard_databases(&shard_databases);
    if let Some(ref flag) = readiness_flag {
        flag.store(true, std::sync::atomic::Ordering::Relaxed);
        tracing::info!("All shards ready — /readyz returning 200");
    }

    // Spawn shard threads
    let mut shard_handles = Vec::with_capacity(num_shards);
    let config_port = config.port;
    for (id, mut shard) in shards.into_iter().enumerate() {
        let producers = mesh.take_producers(id);
        #[allow(unused_mut)]
        let mut consumers = mesh.take_consumers(id);
        // Append admin consumer for this shard (console gateway -> shard SPSC).
        #[cfg(feature = "console")]
        {
            let admin_cons = std::mem::replace(&mut admin_consumers[id], {
                use ringbuf::traits::Split;
                ringbuf::HeapRb::new(1).split().1
            });
            consumers.push(admin_cons);
        }
        let conn_rx = mesh.take_conn_rx(id);
        let shard_cancel = cancel_token.clone();
        let shard_aof_pool = aof_pool.clone();
        let shard_bind_addr = bind_addr.clone();
        let shard_persistence_dir = persistence_dir.clone();
        let shard_snap_rx = snapshot_trigger_rx.clone();
        let shard_snap_tx = snapshot_trigger_tx.clone();
        let shard_repl_state = repl_state.clone();
        let shard_cluster_state = cluster_state.clone();
        let shard_acl_table = acl_table.clone();
        let shard_runtime_config = runtime_config_shared.clone();
        let shard_server_config = server_config_shared.clone();
        let shard_spsc_notify = mesh.take_notify(id);
        let shard_all_notifiers = all_notifiers.clone();
        let shard_tls_config = tls_config.clone();
        let shard_dbs = shard_databases.clone();
        let shard_pubsub_registries = all_pubsub_registries.clone();
        let shard_remote_sub_maps = all_remote_sub_maps.clone();
        let shard_affinity = affinity_tracker.clone();

        let handle = std::thread::Builder::new()
            .name(format!("shard-{}", id))
            .spawn(move || {
                // Pin shard thread to core BEFORE any allocations (NUMA locality)
                moon::shard::numa::pin_to_core(id);

                RuntimeFactoryImpl::block_on_local(format!("shard-{}", id), async move {
                    shard
                        .run(
                            conn_rx,
                            shard_tls_config,
                            consumers,
                            producers,
                            shard_cancel,
                            shard_aof_pool,
                            // Only pass bind_addr for per-shard SO_REUSEPORT when tokio
                            // with io_uring is active. monoio uses central listener MPSC.
                            #[cfg(feature = "runtime-tokio")]
                            {
                                Some(shard_bind_addr)
                            },
                            #[cfg(feature = "runtime-monoio")]
                            {
                                Some(shard_bind_addr)
                            },
                            shard_persistence_dir,
                            shard_snap_rx,
                            shard_snap_tx,
                            Some(shard_repl_state),
                            shard_cluster_state,
                            config_port,
                            shard_acl_table,
                            shard_runtime_config,
                            shard_server_config,
                            shard_spsc_notify,
                            shard_all_notifiers,
                            shard_dbs,
                            shard_pubsub_registries,
                            shard_remote_sub_maps,
                            shard_affinity,
                        )
                        .await;
                });
            })
            .expect("failed to spawn shard thread");

        shard_handles.push(handle);
    }

    // Set up change counter for auto-save
    let change_counter = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));

    let listener_cancel = cancel_token.clone();

    // Run the sharded listener on the main thread.
    // Under tokio: uses current_thread runtime with tokio::spawn for background tasks.
    // Under monoio: uses monoio RuntimeFactory with simplified startup (cluster/gossip
    //   not yet supported under monoio).
    #[cfg(feature = "runtime-tokio")]
    {
        let listener_rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("failed to build listener runtime");

        listener_rt.block_on(async {
            // Set up auto-save timer if save rules are configured (sharded mode)
            if config.save.is_some() {
                let rules = moon::persistence::auto_save::parse_save_rules(&config.save);
                if !rules.is_empty() {
                    let auto_save_token = cancel_token.child_token();
                    let auto_save_counter = change_counter.clone();
                    tokio::spawn(moon::persistence::auto_save::run_auto_save_sharded(
                        rules,
                        auto_save_counter,
                        auto_save_token,
                        snapshot_trigger_tx,
                    ));
                    info!("Auto-save timer started (sharded mode)");
                }
            }

            // Start cluster bus and gossip ticker when cluster mode is enabled
            if let Some(ref cs) = cluster_state {
                let cluster_port = (config.port as u32 + 10000) as u16;
                let cs_clone = cs.clone();
                let bus_cancel = cancel_token.child_token();
                let bind2 = config.bind.clone();
                let self_addr: std::net::SocketAddr =
                    format!("{}:{}", config.bind, config.port).parse().unwrap();

                // Shared vote channel: gossip ticker sets sender when election starts,
                // bus handler forwards FailoverAuthAck votes through it.
                let failover_vote_tx: moon::cluster::bus::SharedVoteTx =
                    std::sync::Arc::new(parking_lot::Mutex::new(None));

                let bus_vote_tx = failover_vote_tx.clone();
                tokio::spawn(async move {
                    if let Err(e) = moon::cluster::bus::run_cluster_bus(
                        &bind2,
                        cluster_port,
                        self_addr,
                        cs_clone,
                        bus_cancel,
                        bus_vote_tx,
                    )
                    .await
                    {
                        tracing::error!("Cluster bus error: {}", e);
                    }
                });

                let cs_gossip = cs.clone();
                let gossip_cancel = cancel_token.child_token();
                let node_timeout = config.cluster_node_timeout;
                let self_addr2: std::net::SocketAddr =
                    format!("{}:{}", config.bind, config.port).parse().unwrap();
                let gossip_vote_tx = failover_vote_tx.clone();
                let gossip_repl_state = repl_state.clone();
                tokio::spawn(async move {
                    moon::cluster::gossip::run_gossip_ticker(
                        self_addr2,
                        cs_gossip,
                        node_timeout,
                        gossip_cancel,
                        gossip_vote_tx,
                        gossip_repl_state,
                    )
                    .await;
                });
                info!("Cluster bus and gossip ticker started");
            }

            // The central tokio listener plain-binds the port (no SO_REUSEPORT,
            // see listener::run_sharded), which makes EVERY per-shard SO_REUSEPORT
            // bind fail with EADDRINUSE — both the io_uring multishot path and the
            // non-uring per_shard_listener fall back to `conn_rx`. `conn_rx`'s only
            // feeder is THIS central accept loop, so it MUST run. Gating it off on
            // Linux (the old `cfg!(target_os = "linux")`) left nobody accepting: the
            // server bound the port and kernel-accepted TCP but never dispatched a
            // command — it hung (the "zombie eating RAM" signature). Keep the central
            // accept loop always on for tokio: identical to the already-working macOS
            // tokio path and to monoio's central-accept model. Per-shard SO_REUSEPORT
            // accept on tokio/Linux additionally rides the io_uring-accept path that
            // is known-fragile under load; central-accept + conn_rx is the proven one.
            // Guarded by tests/multishard_serve_smoke.rs (non-ignored, both runtimes).
            let per_shard_accept = false;
            if let Err(e) = server::listener::run_sharded(
                config,
                conn_txs,
                listener_cancel,
                per_shard_accept,
                affinity_tracker,
            )
            .await
            {
                tracing::error!("Listener error: {}", e);
            }
        });
    }

    #[cfg(feature = "runtime-monoio")]
    {
        // Monoio listener: simplified startup. Cluster bus and gossip not yet
        // supported under monoio.

        // Auto-save runs on a dedicated thread (same pattern as AOF writer).
        if config.save.is_some() {
            let rules = moon::persistence::auto_save::parse_save_rules(&config.save);
            if !rules.is_empty() {
                let auto_save_token = cancel_token.child_token();
                let auto_save_counter = change_counter.clone();
                let snap_tx = snapshot_trigger_tx;
                std::thread::Builder::new()
                    .name("auto-save".to_string())
                    .spawn(move || {
                        RuntimeFactoryImpl::block_on_local(
                            "auto-save".to_string(),
                            moon::persistence::auto_save::run_auto_save_sharded(
                                rules,
                                auto_save_counter,
                                auto_save_token,
                                snap_tx,
                            ),
                        );
                    })
                    .expect("failed to spawn auto-save thread");
                info!("Auto-save timer started (sharded mode, monoio)");
            }
        }

        // monoio: central listener always accepts (per_shard_accept=false).
        // Per-shard SO_REUSEPORT accept is handled by dedicated monoio::spawn() tasks
        // in each shard's event loop (avoids the io_uring cancel/resubmit race in select!).
        // The central listener and per-shard listeners coexist via SO_REUSEPORT:
        // kernel distributes connections across all bound sockets, per-shard handles some
        // directly (no MPSC hop), central forwards the rest via conn_txs.
        let per_shard_accept = false;
        RuntimeFactoryImpl::block_on_local("listener".to_string(), async move {
            if let Err(e) = server::listener::run_sharded(
                config,
                conn_txs,
                listener_cancel,
                per_shard_accept,
                affinity_tracker,
            )
            .await
            {
                tracing::error!("Listener error: {}", e);
            }
        });
    }

    // After listener exits, send AOF shutdown to every writer and cancel all shards.
    // Under TopLevel this is one send; under PerShard (step 2f-β) this fans out to
    // every per-shard writer thread via `broadcast_shutdown`.
    if let Some(ref pool) = aof_pool {
        pool.broadcast_shutdown();
    }
    cancel_token.cancel();
    for handle in shard_handles {
        let _ = handle.join();
    }

    info!("Server shut down");
    Ok(())
}

/// Re-spawn the current process with `_RJEM_MALLOC_CONF=narenas:N` when the
/// operator passes `--memory-arenas-cap N` and N differs from the baked-in
/// default (8).
///
/// tikv-jemallocator uses prefixed symbols (`_rjem_malloc_conf`), so the env
/// var that overrides the config is `_RJEM_MALLOC_CONF` (with `JEMALLOC_CPREFIX`
/// = `_rjem_`). jemalloc reads `opt.narenas` exactly once at init from the
/// symbol **or** the env var (env wins). Calling `mallctl` after init is a
/// documented no-op. Re-spawning via `execve` is the only correct path for a
/// CLI override.
///
/// Sentinel `MOON_ARENAS_CAP_APPLIED=1` prevents infinite re-spawn.
#[cfg(all(feature = "jemalloc", unix))]
fn maybe_respawn_with_arena_override() -> anyhow::Result<()> {
    use std::env;
    use std::os::unix::process::CommandExt;
    const SENTINEL: &str = "MOON_ARENAS_CAP_APPLIED";
    // tikv-jemalloc-sys builds with prefix "_rjem_", so the env var is prefixed.
    const MALLOC_CONF_ENV: &str = "_RJEM_MALLOC_CONF";

    if env::var_os(SENTINEL).is_some() {
        return Ok(());
    }

    // Lightweight scan of argv for --memory-arenas-cap N or --memory-arenas-cap=N.
    // We can't use clap here because clap::parse() requires the full struct, and
    // we need to inject env vars BEFORE jemalloc reads the config.
    // Use args_os() to avoid panicking on non-UTF-8 argv and to preserve the
    // original OsString argv for the re-spawn below (CodeRabbit).
    use std::ffi::OsString;
    use std::os::unix::ffi::OsStrExt;
    let args: Vec<OsString> = env::args_os().collect();
    let mut requested: Option<u32> = None;
    let mut i = 1;
    while i < args.len() {
        let a = args[i].as_os_str().as_bytes();
        if let Some(rest) = a.strip_prefix(b"--memory-arenas-cap=") {
            requested = std::str::from_utf8(rest).ok().and_then(|s| s.parse().ok());
            break;
        }
        if a == b"--memory-arenas-cap" && i + 1 < args.len() {
            requested = args[i + 1]
                .as_os_str()
                .to_str()
                .and_then(|s| s.parse().ok());
            break;
        }
        i += 1;
    }

    // No flag passed -> static _rjem_malloc_conf (narenas:8) is already in effect.
    let Some(n) = requested else {
        return Ok(());
    };
    if n == 8 {
        return Ok(()); // matches default; no override required.
    }

    if env::var_os(MALLOC_CONF_ENV).is_some() {
        // Operator-controlled env var wins; do not clobber.
        eprintln!(
            "WARN: --memory-arenas-cap ignored because {} is already set",
            MALLOC_CONF_ENV
        );
        return Ok(());
    }

    // Rebuild the full config with the requested narenas override.
    let conf_val = format!(
        "narenas:{},background_thread:true,metadata_thp:auto,dirty_decay_ms:1000,muzzy_decay_ms:5000,abort_conf:true",
        n,
    );

    let exe = env::current_exe()?;
    // unix-only: replaces current process image via execve; never returns on success.
    let err = std::process::Command::new(&exe)
        .args(args.iter().skip(1))
        .env(MALLOC_CONF_ENV, &conf_val)
        .env(SENTINEL, "1")
        .exec();
    Err(anyhow::anyhow!(
        "re-spawn for --memory-arenas-cap failed: {}",
        err
    ))
}

/// Resolve the automatic shard count, optionally capped to the empirical
/// knee of 2 when `MOON_AUTO_SHARDS_CONSERVATIVE=1` is set.
///
/// * `parallelism` — value from `available_parallelism()` (or fallback).
/// * `conservative` — when `true`, clamps the result to `min(parallelism, 2)`.
///
/// The cap is intentionally opt-IN: the default `--shards 0` continues to
/// resolve to the full CPU count. Operators on high-core hosts who observe
/// sub-linear multi-shard scaling can set the env var to stay in the
/// `s≤2` sweet spot without changing the startup flag.
pub fn compute_auto_shards(parallelism: usize, conservative: bool) -> usize {
    if conservative {
        parallelism.min(2)
    } else {
        parallelism
    }
}

/// Returns a warning message when the server is configured with too few
/// client slots for the number of shards, or `None` when no warning is
/// needed.
///
/// # Arguments
/// * `maxclients` — configured `--maxclients` value (0 = unlimited; no
///   warning is emitted in that case since there is no per-shard ceiling).
/// * `num_shards` — resolved shard count after auto-detect.
///
/// The empirical threshold is **25 clients per shard**: below this the
/// per-shard SPSC channels are chronically under-subscribed and throughput
/// collapses (documented in `benchmark_scaling_concurrency_2026_04_26`).
///
/// Suppressed entirely when `num_shards == 1` (single-shard has no
/// cross-shard dispatch) or when `maxclients == 0` (unlimited).
pub fn should_warn_undersubscription(maxclients: usize, num_shards: usize) -> Option<String> {
    if num_shards <= 1 || maxclients == 0 {
        return None;
    }
    let threshold = num_shards.saturating_mul(25);
    if maxclients < threshold {
        Some(format!(
            "multi-shard mode with shards={num_shards} expects \
             \u{2265}{threshold} concurrent clients; current \
             maxclients={maxclients} may cause throughput collapse — \
             see CLAUDE.md Gotchas or set MOON_NO_UNDERSUBSCRIPTION_WARN=1 \
             to suppress this warning"
        ))
    } else {
        None
    }
}

#[cfg(not(all(feature = "jemalloc", unix)))]
fn maybe_respawn_with_arena_override() -> anyhow::Result<()> {
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::should_warn_undersubscription;

    #[test]
    fn no_warn_single_shard() {
        assert!(should_warn_undersubscription(100, 1).is_none());
    }

    #[test]
    fn no_warn_unlimited_clients() {
        assert!(should_warn_undersubscription(0, 8).is_none());
    }

    #[test]
    fn no_warn_sufficient_clients() {
        // 8 shards × 25 = 200 → exactly 200 is sufficient
        assert!(should_warn_undersubscription(200, 8).is_none());
    }

    #[test]
    fn warns_below_threshold() {
        // 8 shards × 25 = 200 → 199 is insufficient
        let msg = should_warn_undersubscription(199, 8).expect("should warn");
        assert!(msg.contains("shards=8"), "got: {msg}");
        assert!(msg.contains("maxclients=199"), "got: {msg}");
    }

    #[test]
    fn warns_default_maxclients_low_shards() {
        // 4 shards × 25 = 100 → default maxclients=10000 is fine
        assert!(should_warn_undersubscription(10000, 4).is_none());
    }

    #[test]
    fn warns_high_shard_count() {
        // 32 shards × 25 = 800 → 50 is way below threshold
        let msg = should_warn_undersubscription(50, 32).expect("should warn");
        assert!(msg.contains("shards=32"), "got: {msg}");
    }

    #[test]
    fn threshold_is_inclusive() {
        // exactly at threshold: no warn
        assert!(should_warn_undersubscription(25, 1).is_none()); // single shard
        assert!(should_warn_undersubscription(50, 2).is_none()); // 2×25 = 50
    }

    #[test]
    fn auto_shards_conservative_cap() {
        // verify compute_auto_shards pure function
        assert_eq!(super::compute_auto_shards(16, true), 2);
        assert_eq!(super::compute_auto_shards(16, false), 16);
        assert_eq!(super::compute_auto_shards(1, true), 1);
        assert_eq!(super::compute_auto_shards(4, true), 2);
    }
}
