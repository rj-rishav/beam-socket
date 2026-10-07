//! Startup must reserve both mesh planes on one port and preserve bind errors.

use std::io::ErrorKind;
use std::net::{SocketAddr, TcpListener, UdpSocket};

use beamsocket_mesh::{MeshConfig, MeshNode};

fn config(listen: SocketAddr) -> MeshConfig {
    MeshConfig::new(1, listen, b"cluster-secret".to_vec())
}

#[tokio::test]
async fn ephemeral_start_reserves_tcp_and_udp_on_the_same_port() {
    let node = MeshNode::start(config("127.0.0.1:0".parse().unwrap()))
        .await
        .unwrap();
    let addr = node.addr();
    assert_ne!(addr.port(), 0);
    assert_eq!(
        TcpListener::bind(addr).unwrap_err().kind(),
        ErrorKind::AddrInUse
    );
    assert_eq!(
        UdpSocket::bind(addr).unwrap_err().kind(),
        ErrorKind::AddrInUse
    );
    node.shutdown();
}

#[tokio::test]
async fn occupied_fixed_udp_port_fails_and_releases_tcp_reservation() {
    // Reserve both protocols to obtain a known-free TCP/UDP pair, then leave
    // only UDP occupied. This exercises failure after the mesh TCP bind.
    let (tcp, udp) = loop {
        let tcp = TcpListener::bind("127.0.0.1:0").unwrap();
        match UdpSocket::bind(tcp.local_addr().unwrap()) {
            Ok(udp) => break (tcp, udp),
            Err(error) if error.kind() == ErrorKind::AddrInUse => continue,
            Err(error) => panic!("could not reserve UDP: {error}"),
        }
    };
    let addr = udp.local_addr().unwrap();
    drop(tcp);

    let error = MeshNode::start(config(addr))
        .await
        .err()
        .expect("a fixed occupied UDP port must not be replaced or retried");
    assert_eq!(error.kind(), ErrorKind::AddrInUse);
    let _tcp = TcpListener::bind(addr).expect("failed startup must release TCP");
}
