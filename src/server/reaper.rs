//! Task reaper for cleaning up after an RPC call (or "task") fails.

use capnp;
use gj; // event-driven Asynchronous I/O library

pub struct Reaper;

impl gj::TaskReaper<(), capnp::Error> for Reaper {
    fn task_failed(&mut self, error: capnp::Error) {
        println!("Task failed: {}", error);
    }
}
