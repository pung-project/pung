//! Pung is a private communication system that hides the content of message exchanges
//! as well as the metadata of those exchanges.
extern crate libc;

extern crate rand;
extern crate bit_vec;

extern crate capnp;
extern crate capnp_rpc;
#[macro_use]
extern crate gj;
extern crate gjio;
extern crate byteorder;
extern crate timely;
extern crate timely_communication;
extern crate abomonation;
extern crate crypto;

/// Auto-generated stubs from the [Cap'n Proto RPC protocol](../capnp_rpc/index.html).
pub mod pung_capnp {
    include!(concat!(env!("OUT_DIR"), "/pung_capnp.rs"));
}

#[macro_use]
pub mod util;
pub mod server;
pub mod client;
pub mod db;
pub mod pir;
