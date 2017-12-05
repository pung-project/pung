//! Pung's event-driven RPC server based on [Cap'n Proto remote procedure call protocol]
//! (../../capnp_rpc/index.html). It listens for Pung clients to connect and responds
//! to their Send and Retrieve requests by invoking a distributed computation leveraging
//! [timely dataflow] (../../timely/index.html).
//!
//! #RPC interface
//! The Pung server exposes four RPC calls:
//!
//! **register**: allows clients to register with the Pung server.
//!
//! **sync**: allows clients to obtain the current round number and to create or update their
//! Diffie-Hellman public component and retrieval rate.
//!
//! **send**: allows clients to send a list of [PungTuples](../db/struct.PungTuple.html).
//!
//! **retr**: allows clients to send a retrieval request and obtain a
//! [PungTuple](../db/struct.PungTuple.html).

use capnp;
use capnp_rpc;
use capnp_rpc::{rpc_twoparty_capnp, twoparty, RpcSystem};

// event-loop asynchronous I/O
use gj;
use gjio;

// Pung's Cap'n Proto stubs
use pung_capnp::pung_rpc;

use std;
use std::net::SocketAddr;

// Naiad
use timely::dataflow::scopes::root::Root;
use timely_communication::allocator::generic::Generic;

pub mod timely_shim;
pub mod send_dataflow;
mod rpc;
mod reaper;

use db;
use server::rpc::PungRpc;

fn accept_loop(
    listener: gjio::SocketListener,
    mut task_set: gj::TaskSet<(), capnp::Error>,
    conn: pung_rpc::Client,
) -> gj::Promise<(), std::io::Error> {
    // Accept an incoming connection
    listener.accept().then(move |stream| {
        let mut reader_options: capnp::message::ReaderOptions = Default::default();
        reader_options.traversal_limit_in_words(300 * 1024 * 1024);


        let mut network = twoparty::VatNetwork::new(
            stream.clone(),
            stream,
            rpc_twoparty_capnp::Side::Server,
            reader_options,
        );
        let disconnect_promise = network.on_disconnect();

        // Clone connection for each client, and create rpc context
        let rpc_context = RpcSystem::new(Box::new(network), Some(conn.clone().client));

        // Add the rpc conext + connection to the set of tasks
        task_set.add(disconnect_promise.attach(rpc_context));

        // Go back to accepting other connections
        accept_loop(listener, task_set, conn)
    })
}

/// Launches an RPC server that interfaces with timely dataflow via
/// a [shim layer](timely_shim/index.html) that captures inputs and outputs.
/// The RPC server is in charge of passing inputs to timely dataflow via
/// [`timely::dataflow::operators::input::send`]
/// (../../timely/dataflow/operators/input/struct.Handle.html).
/// The RPC server is also required to instruct the timely worker to
/// perform computational steps on the provided data via calls to step in
/// [timely::dataflow::scopes::root::Root](../../timely/dataflow/scopes/root/struct.Root.html).
pub fn run_rpc(
    addr: SocketAddr,
    worker: Root<Generic>,
    send: timely_shim::SendHandler,
    dbase: db::DatabasePtr,
    extra_tuples: usize,
    min_messages: u32,
    opt_scheme: db::OptScheme,
) {
    // Event-loop for RPC. This never returns.

    gj::EventLoop::top_level(move |wait_scope| -> Result<(), capnp::Error> {
        // create event port
        let mut event_port = try!(gjio::EventPort::new());
        let network = event_port.get_network();
        let mut address = network.get_tcp_address(addr);

        // create a listener for Pung's RPC server
        let listener = try!(address.listen());

        // instance of the pung RPC server
        let connection = pung_rpc::ToClient::new(PungRpc::new(
            worker,
            send,
            dbase,
            extra_tuples,
            min_messages,
            opt_scheme,
        )).from_server::<capnp_rpc::Server>();

        // defines a set that holds all promises ("tasks") and a destructor in case they go awry
        let task_set = gj::TaskSet::new(Box::new(reaper::Reaper));

        try!(accept_loop(listener, task_set, connection).wait(wait_scope, &mut event_port));

        Ok(())
    }).expect("top level error running server RPC");
}
