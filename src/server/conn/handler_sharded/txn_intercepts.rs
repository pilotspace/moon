//! Connection-level intercepts, extracted so `EXEC` can run them too (moon#639).
//!
//! These five families used to live inline in the handler's read loop. They are
//! functions now for one reason: a command queued inside `MULTI` is executed by
//! `EXEC`, and `EXEC` needs the *same* code the live path runs. A second copy
//! would drift — the queued form would answer differently from the live form,
//! and no single test compares the two.
//!
//! The signatures mirror the live call sites exactly, including pushing to a
//! plain `Vec<Frame>` rather than `InterceptReplies`: that is what these blocks
//! did before, and changing the RESP3 shaping of five commands is not this
//! change's business.

use bytes::Bytes;

use crate::command::connection as conn_cmd;
use crate::protocol::Frame;
use crate::server::conn::core::{ConnectionContext, ConnectionState};

/// `AUTH` — post-authentication path (the unauthenticated one is the gate).
pub(super) fn try_handle_auth(
    cmd: &[u8],
    cmd_args: &[Frame],
    conn: &mut ConnectionState,
    ctx: &ConnectionContext,
    peer_addr: &str,
    auth_delay_ms: &mut u64,
    responses: &mut Vec<Frame>,
) -> bool {
    if !cmd.eq_ignore_ascii_case(b"AUTH") {
        return false;
    }
    let (response, opt_user) = conn_cmd::auth_acl(cmd_args, &ctx.acl_table);
    if let Some(uname) = opt_user {
        conn.adopt_user(uname, &ctx.acl_table);
        if let Ok(addr) = peer_addr.parse::<std::net::SocketAddr>() {
            crate::auth_ratelimit::record_success(addr.ip());
        }
    } else if let Ok(addr) = peer_addr.parse::<std::net::SocketAddr>() {
        *auth_delay_ms += crate::auth_ratelimit::record_failure(addr.ip());
    }
    responses.push(response);
    true
}

/// `HELLO`. `switch_index` is the position in the OUTER response vector after
/// which the new protocol takes effect — the live path passes `responses.len()`
/// so the HELLO reply itself is still written in the old protocol.
#[allow(clippy::too_many_arguments)]
pub(super) fn try_handle_hello(
    cmd: &[u8],
    cmd_args: &[Frame],
    conn: &mut ConnectionState,
    ctx: &ConnectionContext,
    client_id: u64,
    peer_addr: &str,
    auth_delay_ms: &mut u64,
    switch_index: usize,
    responses: &mut Vec<Frame>,
) -> bool {
    if !cmd.eq_ignore_ascii_case(b"HELLO") {
        return false;
    }
    let (response, new_proto, new_name, opt_user) = conn_cmd::hello_acl(
        cmd_args,
        conn.protocol_version,
        client_id,
        &ctx.acl_table,
        &mut conn.authenticated,
        crate::command::identity::hello_role_and_mode(
            ctx.repl_state.as_ref(),
            ctx.cluster_state.is_some(),
        ),
    );
    if !matches!(&response, Frame::Error(_)) {
        crate::server::conn::shared::note_protocol_switch(conn, switch_index, new_proto);
        conn.protocol_version = new_proto;
    }
    if let Some(name) = new_name {
        conn.client_name = Some(name);
    }
    if let Some(ref uname) = opt_user {
        conn.adopt_user(uname.to_string(), &ctx.acl_table);
    }
    if matches!(&response, Frame::Error(_)) {
        if let Ok(addr) = peer_addr.parse::<std::net::SocketAddr>() {
            *auth_delay_ms += crate::auth_ratelimit::record_failure(addr.ip());
        }
    } else if opt_user.is_some()
        && let Ok(addr) = peer_addr.parse::<std::net::SocketAddr>()
    {
        crate::auth_ratelimit::record_success(addr.ip());
    }
    responses.push(response);
    true
}

/// `CLUSTER`, including the `CLUSTER REPLICATE` side effect of actually
/// starting the replica task rather than only relabelling the node.
pub(super) fn try_handle_cluster(
    cmd: &[u8],
    cmd_args: &[Frame],
    ctx: &ConnectionContext,
    responses: &mut Vec<Frame>,
) -> bool {
    if !cmd.eq_ignore_ascii_case(b"CLUSTER") {
        return false;
    }
    let Some(ref cs) = ctx.cluster_state else {
        responses.push(Frame::Error(Bytes::from_static(
            b"ERR This instance has cluster support disabled",
        )));
        return true;
    };
    #[allow(clippy::unwrap_used)] // Fallback "127.0.0.1:6379" is a valid literal
    let self_addr: std::net::SocketAddr = format!("127.0.0.1:{}", ctx.config_port)
        .parse()
        .unwrap_or_else(|_| "127.0.0.1:6379".parse().unwrap());
    let resp = crate::cluster::command::handle_cluster_command(cmd_args, cs, self_addr);
    if matches!(resp, Frame::SimpleString(ref ok) if ok.as_ref() == b"OK")
        && let Some((host, port)) = crate::cluster::command::cluster_replicate_target(cmd_args, cs)
        && let Some(ref rs) = ctx.repl_state
    {
        rs.write()
            .set_role(crate::replication::state::ReplicationRole::Replica {
                host: host.clone(),
                port,
                state: crate::replication::handshake::ReplicaHandshakeState::PingPending,
            });
        let epoch = crate::replication::replica::bump_replica_task_epoch();
        let cfg = crate::replication::replica::ReplicaTaskConfig {
            master_host: host,
            master_port: port,
            repl_state: std::sync::Arc::clone(rs),
            num_shards: ctx.num_shards,
            persistence_dir: None,
            listening_port: 0,
            epoch,
            stream_db: std::sync::atomic::AtomicUsize::new(0),
            shard_databases: ctx.shard_databases.clone(),
        };
        tokio::task::spawn_local(crate::replication::replica::run_replica_task(cfg));
    }
    responses.push(resp);
    true
}

/// `SCRIPT`, with the bounded shard fan-out `SCRIPT LOAD` needs.
pub(super) async fn try_handle_script(
    cmd: &[u8],
    cmd_args: &[Frame],
    ctx: &ConnectionContext,
    shutdown: &crate::runtime::cancel::CancellationToken,
    responses: &mut Vec<Frame>,
) -> bool {
    if !cmd.eq_ignore_ascii_case(b"SCRIPT") {
        return false;
    }
    let (response, fanout) =
        crate::scripting::handle_script_subcommand(&ctx.script_cache, cmd_args);
    if let Some((sha1, script_bytes)) = fanout {
        // E3: bounded fan-out — a full ring no longer silently diverges that
        // shard's script cache.
        crate::server::conn::shared::script_fanout_bounded(ctx, shutdown, &sha1, &script_bytes)
            .await;
    }
    responses.push(response);
    true
}

/// `ACL`.
pub(super) fn try_handle_acl(
    cmd: &[u8],
    cmd_args: &[Frame],
    client_id: u64,
    conn: &mut ConnectionState,
    ctx: &ConnectionContext,
    peer_addr: &str,
    responses: &mut Vec<Frame>,
) -> bool {
    if !cmd.eq_ignore_ascii_case(b"ACL") {
        return false;
    }
    let response = crate::command::acl::handle_acl(
        cmd_args,
        &ctx.acl_table,
        &mut conn.acl_log,
        &conn.current_user,
        peer_addr,
        &ctx.runtime_config,
        client_id,
    );
    responses.push(response);
    true
}

/// Run one queued connection-level intercept at `EXEC` time. The sharded twin
/// of `handler_monoio::dispatch::run_txn_connection_intercept`; see that
/// function for why `EXEC` rather than the executor runs these.
#[allow(clippy::too_many_arguments)]
pub(super) async fn run_txn_connection_intercept(
    cmd: &[u8],
    cmd_args: &[Frame],
    client_id: u64,
    conn: &mut ConnectionState,
    ctx: &ConnectionContext,
    peer_addr: &str,
    shutdown: &crate::runtime::cancel::CancellationToken,
    switch_index: usize,
    func_registry: &std::rc::Rc<std::cell::RefCell<Option<crate::scripting::FunctionRegistry>>>,
) -> Frame {
    let mut out: Vec<Frame> = Vec::with_capacity(1);
    // Dropped: inside EXEC there is no read loop to slow down, and sleeping
    // here would stall the rest of the transaction's replies.
    let mut auth_delay_ms: u64 = 0;

    let handled = try_handle_auth(
        cmd,
        cmd_args,
        conn,
        ctx,
        peer_addr,
        &mut auth_delay_ms,
        &mut out,
    ) || try_handle_hello(
        cmd,
        cmd_args,
        conn,
        ctx,
        client_id,
        peer_addr,
        &mut auth_delay_ms,
        switch_index,
        &mut out,
    ) || try_handle_cluster(cmd, cmd_args, ctx, &mut out)
        || try_handle_script(cmd, cmd_args, ctx, shutdown, &mut out).await
        || try_handle_acl(cmd, cmd_args, client_id, conn, ctx, peer_addr, &mut out)
        || super::dispatch::try_handle_config(
            cmd,
            cmd_args,
            ctx,
            &mut crate::server::conn::intercept::InterceptReplies::new(
                &mut out,
                cmd,
                cmd_args,
                conn.protocol_version,
            ),
        )
        || super::dispatch::try_handle_wait(
            cmd,
            cmd_args,
            ctx,
            &mut crate::server::conn::intercept::InterceptReplies::new(
                &mut out,
                cmd,
                cmd_args,
                conn.protocol_version,
            ),
        )
        .await
        || {
            let proto = conn.protocol_version;
            super::dispatch::try_handle_client_command(
                cmd,
                cmd_args,
                client_id,
                conn,
                ctx,
                &mut crate::server::conn::intercept::InterceptReplies::new(
                    &mut out, cmd, cmd_args, proto,
                ),
            )
        }
        || super::pubsub::try_handle_pubsub_introspection(cmd, cmd_args, ctx, &mut out)
        // moon#697: ONE implementation shared with the monoio twin. Two copies of
        // a fan-out this subtle is exactly how the paths drifted before.
        || crate::server::conn::shared::try_handle_function_in_txn(
            cmd,
            cmd_args,
            ctx,
            shutdown,
            func_registry,
            &mut out,
        )
        .await;

    if !handled {
        return Frame::Error(Bytes::from(format!(
            "ERR unknown command '{}' inside MULTI/EXEC",
            String::from_utf8_lossy(cmd)
        )));
    }
    out.pop().unwrap_or_else(|| {
        Frame::Error(Bytes::from_static(
            b"ERR intercept produced no reply inside MULTI/EXEC",
        ))
    })
}

/// Fill the placeholders the executor left. Sharded twin of
/// `handler_monoio::dispatch::fill_txn_intercept_slots` — see it for why an
/// aborted `EXEC` must leave the intercepts unrun.
#[allow(clippy::too_many_arguments)]
pub(super) async fn fill_txn_intercept_slots(
    result: &mut Frame,
    queue: &[Frame],
    conn: &mut ConnectionState,
    ctx: &ConnectionContext,
    shutdown: &crate::runtime::cancel::CancellationToken,
    switch_index: usize,
    func_registry: &std::rc::Rc<std::cell::RefCell<Option<crate::scripting::FunctionRegistry>>>,
) {
    let Frame::Array(results) = result else {
        return;
    };
    if !queue
        .iter()
        .filter_map(crate::server::conn::util::extract_command)
        .any(|(c, _)| crate::server::conn::shared::is_txn_connection_intercept(c))
    {
        return;
    }
    let client_id = conn.client_id;
    let peer_addr = conn.peer_addr.clone();
    for (i, frame) in queue.iter().enumerate() {
        if i >= results.len() {
            break;
        }
        let Some((c, a)) = crate::server::conn::util::extract_command(frame) else {
            continue;
        };
        if !crate::server::conn::shared::is_txn_connection_intercept(c) {
            continue;
        }
        results[i] = run_txn_connection_intercept(
            c,
            a,
            client_id,
            conn,
            ctx,
            &peer_addr,
            shutdown,
            switch_index,
            func_registry,
        )
        .await;
    }
}
