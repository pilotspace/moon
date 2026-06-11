//! Socket options applied to accepted client connections.

/// Enable TCP_NODELAY on an accepted client socket (QW1, 2026-06 review
/// finding 3.2). Redis defaults to `tcp-nodelay yes`; without it Nagle can
/// hold pipelined responses in the kernel send buffer for a delayed-ACK
/// window, turning sub-millisecond operations into ~40ms latency spikes.
///
/// Generic over anything exposing the socket fd (`std`/`tokio` `TcpStream`,
/// `BorrowedFd` from the monoio/uring accept paths).
#[cfg(unix)]
pub fn apply_client_socket_opts<S: std::os::fd::AsFd>(sock: &S) -> std::io::Result<()> {
    let sock_ref = socket2::SockRef::from(sock);
    sock_ref.set_tcp_nodelay(true)
}

#[cfg(test)]
mod tests {
    #[test]
    #[cfg(unix)]
    fn sets_nodelay_on_accepted_socket() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let _client = std::net::TcpStream::connect(addr).unwrap();
        let (accepted, _) = listener.accept().unwrap();
        assert!(!accepted.nodelay().unwrap());
        super::apply_client_socket_opts(&accepted).unwrap();
        assert!(accepted.nodelay().unwrap());
    }
}
