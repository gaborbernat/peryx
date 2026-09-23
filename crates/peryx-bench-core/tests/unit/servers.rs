#[cfg(unix)]
#[test]
fn killing_a_reaped_process_group_reports_the_missing_group() {
    use std::os::unix::process::CommandExt as _;

    let mut command = super::Command::new("sh");
    command.args(["-c", "exit 0"]).process_group(0);
    let mut process = command.spawn().expect("the fixture starts");
    process.wait().expect("the fixture exits");
    assert_eq!(
        super::kill_process_group(&process)
            .expect_err("a reaped group cannot be signalled")
            .raw_os_error(),
        Some(rustix::io::Errno::SRCH.raw_os_error())
    );
}

#[tokio::test]
async fn a_server_that_goes_silent_fails_the_request() {
    let listener = std::net::TcpListener::bind(("127.0.0.1", 0)).expect("the host has a free port");
    let url = format!(
        "http://{}/",
        listener.local_addr().expect("the listener has an address")
    );
    let client = super::client_with_timeouts(super::CONNECT_TIMEOUT, super::Duration::from_millis(50))
        .expect("the client builds");
    assert!(client.get(url).send().await.expect_err("nothing answers").is_timeout());
}

#[test]
fn a_free_port_sits_below_every_ephemeral_range() {
    assert!(super::SERVER_PORTS.contains(&super::free_port().expect("the host has a free port")));
}

#[test]
fn consecutive_free_ports_differ() {
    assert_ne!(
        super::free_port().expect("the host has a free port"),
        super::free_port().expect("the host has a free port")
    );
}

#[test]
fn a_taken_range_reports_no_free_port() {
    let held = std::net::TcpListener::bind(("127.0.0.1", 0)).expect("the host has a free port");
    let port = held.local_addr().expect("the listener has an address").port();
    assert_eq!(
        super::free_port_in(port..port + 1)
            .expect_err("the only port is held")
            .to_string(),
        format!("no free port in {port}..{}", port + 1)
    );
}
