//! A set of structures that allows a Pung RPC server to interface with
//! [timely dataflow](../../../timely/index.html).


use capnp::Error;

use db::PungTuple;
use gj;
use std::cell::RefCell;
use std::rc::Rc;

use timely::dataflow::operators::{input, probe};
use timely::progress::nested::product::Product;
use timely::progress::timestamp::RootTimestamp;

pub type SendFulfiller = gj::PromiseFulfiller<Rc<(Vec<u64>, Vec<Vec<u8>>)>, Error>;
pub type SendFulfillerList = Rc<RefCell<Vec<SendFulfiller>>>;

/// Handler used by the RPC server to interface with [timely dataflow]
/// (../../../timely/index.html) during Pung's send phase.
pub struct SendHandler {
    /// input handle for a given round for passing PungTuples to the timely dataflow system
    pub input: input::Handle<usize, PungTuple>,

    /// allows the RPC server to check on the progress of a given round.
    pub probe: probe::Handle<Product<RootTimestamp, usize>>,

    /// shared pointer to thestate of a send round (promises)
    pub fulfillers: SendFulfillerList,
}
