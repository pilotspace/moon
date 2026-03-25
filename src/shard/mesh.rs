use std::sync::Arc;

use ringbuf::traits::Split;
use ringbuf::HeapCons;
use ringbuf::HeapProd;
use ringbuf::HeapRb;
use tokio::sync::mpsc;

use super::dispatch::ShardMessage;

/// Default SPSC buffer size (entries, not bytes). 4096 entries.
pub const CHANNEL_BUFFER_SIZE: usize = 4096;

/// Connection channel capacity for listener -> shard.
pub const CONN_CHANNEL_CAPACITY: usize = 256;

/// SPSC channel mesh connecting all shard pairs.
///
/// For N shards, creates N*(N-1) unidirectional SPSC channels.
/// Each shard gets (N-1) producers (to send to every other shard)
/// and (N-1) consumers (to receive from every other shard),
/// plus one connection channel from the listener.
///
/// Index mapping within each shard's producer/consumer Vec:
///   - For shard `my_id` communicating with shard `target_id`:
///     - If `target_id < my_id`: index = `target_id`
///     - If `target_id > my_id`: index = `target_id - 1`
///   - Use `ChannelMesh::target_index(my_id, target_id)` for this mapping.
pub struct ChannelMesh {
    /// producers[sender_shard] = Vec of producers, one per other shard.
    /// Index within the Vec corresponds to target shard with skip-self mapping.
    producers: Vec<Vec<HeapProd<ShardMessage>>>,
    /// consumers[receiver_shard] = Vec of consumers, one per other shard.
    consumers: Vec<Vec<HeapCons<ShardMessage>>>,
    /// Connection channels: listener sends TcpStream to each shard.
    conn_txs: Vec<mpsc::Sender<tokio::net::TcpStream>>,
    conn_rxs: Vec<Option<mpsc::Receiver<tokio::net::TcpStream>>>,
    /// Per-shard Notify instances for event-driven SPSC wake.
    /// Producers call `notify_one()` after pushing to wake the target shard immediately.
    spsc_notifiers: Vec<Arc<tokio::sync::Notify>>,
    num_shards: usize,
}

impl ChannelMesh {
    /// Create a new channel mesh for `n` shards.
    ///
    /// Each shard pair gets one SPSC channel in each direction (N*(N-1) total).
    /// Connection channels use tokio mpsc (single listener -> each shard).
    pub fn new(n: usize, buffer_size: usize) -> Self {
        // Initialize empty vecs for producers and consumers
        let mut producers: Vec<Vec<HeapProd<ShardMessage>>> = Vec::with_capacity(n);
        let mut consumers: Vec<Vec<HeapCons<ShardMessage>>> = Vec::with_capacity(n);

        for _ in 0..n {
            producers.push(Vec::with_capacity(n.saturating_sub(1)));
            consumers.push(Vec::with_capacity(n.saturating_sub(1)));
        }

        // Create N*(N-1) unidirectional SPSC channels.
        // For each ordered pair (sender, receiver) where sender != receiver:
        // The producer goes to producers[sender], the consumer goes to consumers[receiver].
        //
        // Iteration order matters: for each sender, we iterate receivers in order
        // 0..n skipping sender, which produces the correct skip-self index mapping.
        for sender in 0..n {
            for receiver in 0..n {
                if sender == receiver {
                    continue;
                }
                let rb = HeapRb::new(buffer_size);
                let (prod, cons) = rb.split();
                producers[sender].push(prod);
                consumers[receiver].push(cons);
            }
        }

        // Connection channels: tokio mpsc for listener -> shard distribution.
        let mut conn_txs = Vec::with_capacity(n);
        let mut conn_rxs = Vec::with_capacity(n);
        for _ in 0..n {
            let (tx, rx) = mpsc::channel(CONN_CHANNEL_CAPACITY);
            conn_txs.push(tx);
            conn_rxs.push(Some(rx));
        }

        // Per-shard Notify for event-driven SPSC wake (replaces 1ms polling).
        let spsc_notifiers: Vec<Arc<tokio::sync::Notify>> =
            (0..n).map(|_| Arc::new(tokio::sync::Notify::new())).collect();

        ChannelMesh {
            producers,
            consumers,
            conn_txs,
            conn_rxs,
            spsc_notifiers,
            num_shards: n,
        }
    }

    /// Take the producers for a specific shard (call once during setup).
    ///
    /// Returns Vec of producers indexed by target shard (skip-self mapping).
    /// Use `target_index(my_id, target_id)` to find the correct index.
    pub fn take_producers(&mut self, shard_id: usize) -> Vec<HeapProd<ShardMessage>> {
        std::mem::take(&mut self.producers[shard_id])
    }

    /// Take the consumers for a specific shard (call once during setup).
    ///
    /// Returns Vec of consumers indexed by source shard (skip-self mapping).
    pub fn take_consumers(&mut self, shard_id: usize) -> Vec<HeapCons<ShardMessage>> {
        std::mem::take(&mut self.consumers[shard_id])
    }

    /// Take the connection receiver for a specific shard (call once during setup).
    ///
    /// Panics if called more than once for the same shard.
    pub fn take_conn_rx(&mut self, shard_id: usize) -> mpsc::Receiver<tokio::net::TcpStream> {
        self.conn_rxs[shard_id]
            .take()
            .expect("conn_rx already taken")
    }

    /// Get a clone of the connection sender for a specific shard.
    ///
    /// The listener uses this to distribute new connections to shards.
    pub fn conn_tx(&self, shard_id: usize) -> mpsc::Sender<tokio::net::TcpStream> {
        self.conn_txs[shard_id].clone()
    }

    /// Get the Notify handle for a specific shard (used by the shard's own event loop).
    pub fn take_notify(&self, shard_id: usize) -> Arc<tokio::sync::Notify> {
        self.spsc_notifiers[shard_id].clone()
    }

    /// Get all notifiers (used by connection handlers to wake target shards after SPSC push).
    pub fn all_notifiers(&self) -> Vec<Arc<tokio::sync::Notify>> {
        self.spsc_notifiers.clone()
    }

    /// Number of shards in this mesh.
    pub fn num_shards(&self) -> usize {
        self.num_shards
    }

    /// Map a target shard ID to the index within a shard's producer/consumer Vec.
    ///
    /// For shard `my_id` communicating with `target_id`:
    ///   - if `target_id < my_id`: index = `target_id`
    ///   - if `target_id > my_id`: index = `target_id - 1`
    ///
    /// # Panics
    /// Debug-asserts that `my_id != target_id` (sending to self is invalid).
    #[inline]
    pub fn target_index(my_id: usize, target_id: usize) -> usize {
        debug_assert_ne!(my_id, target_id, "cannot send to self");
        if target_id < my_id {
            target_id
        } else {
            target_id - 1
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ringbuf::traits::{Consumer, Producer};

    #[test]
    fn test_mesh_creation() {
        // Creating a mesh for 4 shards with buffer size 64 should not panic
        let mesh = ChannelMesh::new(4, 64);
        assert_eq!(mesh.num_shards(), 4);
    }

    #[test]
    fn test_mesh_single_shard() {
        // Single shard: 0 inter-shard channels, 1 connection channel
        let mut mesh = ChannelMesh::new(1, 64);
        let producers = mesh.take_producers(0);
        let consumers = mesh.take_consumers(0);
        assert_eq!(producers.len(), 0);
        assert_eq!(consumers.len(), 0);
    }

    #[test]
    fn test_target_index() {
        // Shard 0 sending to shard 1: target > my_id, index = 1 - 1 = 0
        assert_eq!(ChannelMesh::target_index(0, 1), 0);
        // Shard 0 sending to shard 2: target > my_id, index = 2 - 1 = 1
        assert_eq!(ChannelMesh::target_index(0, 2), 1);
        // Shard 2 sending to shard 0: target < my_id, index = 0
        assert_eq!(ChannelMesh::target_index(2, 0), 0);
        // Shard 2 sending to shard 1: target < my_id, index = 1
        assert_eq!(ChannelMesh::target_index(2, 1), 1);
        // Shard 2 sending to shard 3: target > my_id, index = 3 - 1 = 2
        assert_eq!(ChannelMesh::target_index(2, 3), 2);
        // Shard 1 sending to shard 0: target < my_id, index = 0
        assert_eq!(ChannelMesh::target_index(1, 0), 0);
        // Shard 1 sending to shard 2: target > my_id, index = 2 - 1 = 1
        assert_eq!(ChannelMesh::target_index(1, 2), 1);
    }

    #[test]
    fn test_producer_consumer_counts() {
        let n = 4;
        let mut mesh = ChannelMesh::new(n, 64);
        for i in 0..n {
            let prods = mesh.take_producers(i);
            assert_eq!(
                prods.len(),
                n - 1,
                "shard {} should have {} producers",
                i,
                n - 1
            );
        }
        // Re-create mesh to test consumers (take_producers consumed them)
        let mut mesh = ChannelMesh::new(n, 64);
        for i in 0..n {
            let cons = mesh.take_consumers(i);
            assert_eq!(
                cons.len(),
                n - 1,
                "shard {} should have {} consumers",
                i,
                n - 1
            );
        }
    }

    #[test]
    fn test_spsc_send_receive() {
        // Verify that producers and consumers are correctly paired:
        // shard 0's producer[target_index(0,1)] should connect to shard 1's consumer[target_index(1,0)]
        let n = 3;
        let mut mesh = ChannelMesh::new(n, 64);

        let mut all_prods: Vec<Vec<HeapProd<ShardMessage>>> = Vec::new();
        let mut all_cons: Vec<Vec<HeapCons<ShardMessage>>> = Vec::new();
        for i in 0..n {
            all_prods.push(mesh.take_producers(i));
            all_cons.push(mesh.take_consumers(i));
        }

        // Shard 0 sends to shard 1 via producer at target_index(0, 1) = 0
        let idx = ChannelMesh::target_index(0, 1);
        all_prods[0][idx]
            .try_push(ShardMessage::Shutdown)
            .ok()
            .expect("push should succeed");

        // Shard 1 receives from shard 0 via consumer at target_index(1, 0) = 0
        let recv_idx = ChannelMesh::target_index(1, 0);
        let msg = all_cons[1][recv_idx].try_pop();
        assert!(msg.is_some(), "should receive message from shard 0");
        assert!(
            matches!(msg.unwrap(), ShardMessage::Shutdown),
            "should be Shutdown"
        );
    }

    #[test]
    fn test_conn_channels() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async {
            let mut mesh = ChannelMesh::new(4, 64);
            // Take conn_rx for shard 0
            let mut rx = mesh.take_conn_rx(0);
            // Get conn_tx for shard 0
            let tx = mesh.conn_tx(0);

            // Create a TCP listener to produce a TcpStream
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap();

            // Connect and send the stream
            let connect = tokio::net::TcpStream::connect(addr);
            let accept = listener.accept();
            let (client_res, accept_res) = tokio::join!(connect, accept);
            let client = client_res.unwrap();
            let (_server, _addr) = accept_res.unwrap();

            tx.send(client).await.unwrap();
            let received = rx.recv().await;
            assert!(received.is_some(), "should receive TcpStream");
        });
    }

    #[test]
    #[should_panic(expected = "conn_rx already taken")]
    fn test_conn_rx_double_take_panics() {
        let mut mesh = ChannelMesh::new(2, 64);
        let _rx1 = mesh.take_conn_rx(0);
        let _rx2 = mesh.take_conn_rx(0); // should panic
    }
}
