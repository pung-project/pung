// This deals with broadcasting the data across all workers

use db;
use server::timely_shim;
use std::cell::RefCell;
use std::rc::Rc;

// Naiad libraries
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::*;
use timely::dataflow::scopes::root::Root;
use timely_communication::allocator::generic::Generic;
use util;

pub fn graph(worker: &mut Root<Generic>, dbase: db::DatabasePtr, buckets: usize) -> timely_shim::SendHandler {

    let fulfillers: timely_shim::SendFulfillerList = Rc::new(RefCell::new(Vec::new()));
    let send_fulfillers = fulfillers.clone();

    let mut partitions: Vec<Vec<u8>> = Vec::with_capacity(buckets);

    for i in 0..buckets {
        partitions.push(util::label_marker(i, buckets));
    }

    let (input, probe) = worker.dataflow(move |dataflow| {

        // Get input from RPCs
        let (s_input, stream) = dataflow.new_input::<db::PungTuple>();

        let s_probe = stream
            .broadcast()  // broadcast received Tuples to all workers
            .unary_notify(Pipeline, "build-db", vec![], move |input, output, notificator| {

                let db = &mut dbase.borrow_mut();

                // Process the tuples sent by other workers (and our own tuples)
                input.for_each(|time, data| {
                    notificator.notify_at(time);

                    // Add tuples to the database
                    for datum in data.drain(..) {
                        for (i, label) in partitions.iter().enumerate() {
                            if datum.label() <= label {
                                // Push to bucket i
                                db.push(i, datum);
                                break;
                            }
                        }
                    }

                });

                // Get the fulfillers for this worker's clients
                let f_list = &mut send_fulfillers.borrow_mut();

                notificator.for_each(|time, _num, _notify| {

                    let mut buckets_len = Vec::with_capacity(db.num_buckets());
                    let mut buckets_lmid: Vec<Vec<u8>> = Vec::new();

                    // Encode each collection: BST + batch codes
                    db.encode();

                    // Number of tuples in each bucket (and lmid if applicable)
                    for bucket in db.get_buckets() {
                        buckets_len.push(bucket.unencoded_len() as u64);

                        if db.opt_scheme() >= db::OptScheme::Hybrid2 {
                            buckets_lmid.extend(bucket.mid_labels());
                        }
                    }

                    // Result to be given to clients
                    let buckets_info = Rc::new((buckets_len, buckets_lmid));

                    // Notify each client of this worker the value of n
                    for f in f_list.drain(..) {
                        f.fulfill(buckets_info.clone());
                    }

                    // Setup PIR for each collection in the database
                    db.pir_setup();

                    output.session(&time).give(0);
              });
            })
            .probe();

        (s_input, s_probe)
    });

    timely_shim::SendHandler { input: input, probe: probe, fulfillers: fulfillers }
}
