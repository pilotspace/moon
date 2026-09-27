#![no_main]
use libfuzzer_sys::fuzz_target;

use moon::graph::replay::GraphReplayCollector;
use moon::graph::store::GraphStore;

// Fuzz the graph WAL replay collector (moon#1285).
//
// `GraphReplayCollector::collect_command` parses every `GRAPH.*` record a
// restart reads back from the WAL and every graph record a replica receives
// on its replication link — `GRAPH.CREATE/ADDNODE/ADDEDGE/REMOVENODE/
// REMOVEEDGE/SETPROP/SETLABEL/DROP` and, since moon#1285, the `TXN.ABORT`
// rollback records `GRAPH.DELPROP`, `GRAPH.UNDELETENODE <n> <ids..>` and
// `GRAPH.UNDELETEEDGE`. A torn or corrupted record must be refused, never
// panic; `replay_into` must then apply whatever was accepted — in any order,
// against ids that may not exist — without panicking either.
//
// Input: up to 8 records separated by `\n`; within a record, arguments are
// separated by `\x00`, the first being the command name (a leading
// `GRAPH.` prefix is supplied when absent, so the fuzzer reaches the arms).
fuzz_target!(|data: &[u8]| {
    let mut collector = GraphReplayCollector::new();
    for record in data.split(|b| *b == b'\n').take(8) {
        let mut parts = record.split(|b| *b == 0);
        let Some(name) = parts.next() else {
            continue;
        };
        let mut cmd = Vec::with_capacity(6 + name.len());
        if !name.starts_with(b"GRAPH.") {
            cmd.extend_from_slice(b"GRAPH.");
        }
        cmd.extend_from_slice(name);
        let args: Vec<&[u8]> = parts.take(64).collect();
        let _ = GraphReplayCollector::is_graph_command(&cmd);
        let _ = collector.collect_command(&cmd, &args);
    }
    let mut store = GraphStore::new();
    let _ = collector.replay_into(&mut store);
});
