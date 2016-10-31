// Implementation of the server's RPC call (each timely dataflow worker is an RPC server)

use capnp::Error;

use db;
use gj;

// RPC Stubs
use pung_capnp::pung_rpc;
use pung_capnp::pung_rpc::{CloseParams, CloseResults, GetMappingParams, GetMappingResults, RegisterParams,
                           RegisterResults, RetrParams, RetrResults, SendParams, SendResults, SyncParams,
                           SyncResults, GetBloomParams, GetBloomResults, ChangeExtraParams, ChangeExtraResults};

use rand::ChaChaRng;
use rand::Rng;
use server::timely_shim;
use std::collections::HashMap;
use std::rc::Rc;

// Naiad libraries
use timely::dataflow::scopes::root::Root;
use timely::progress::timestamp::RootTimestamp;
use timely_communication::allocator::generic::Generic;

use util;


#[derive(PartialEq)]
enum Phase {
    Sending,
    Receiving,
}

struct SendCtx {
    reqs: HashMap<u64, u32>, // client id -> requests received so far
    // map from round number to (id, tuple, fulfiller) tuple for queuing requests
    queue: HashMap<u64, Vec<(u64, Vec<db::PungTuple>, timely_shim::SendFulfiller)>>,
    handler: timely_shim::SendHandler,
    count: u32,
}

struct RetCtx {
    reqs: HashMap<u64, u32>, // client id -> requests received so far
}

pub struct PungRpc {
    round: u64,
    clients: HashMap<u64, u32>, // client id -> request rate

    worker: Root<Generic>,

    phase: Phase,
    send_ctx: SendCtx,
    ret_ctx: RetCtx,

    dbase: db::DatabasePtr,

    extra_tuples: Vec<db::PungTuple>, // blows up the collection size by extra_tuples.len()

    min_messages: u32, // hack to prevent server from advancing round until all clients have sent
    opt_scheme: db::OptScheme,
}


impl PungRpc {
    pub fn new(worker: Root<Generic>,
               send: timely_shim::SendHandler,
               dbase: db::DatabasePtr,
               extra: usize,
               min_messages: u32,
               opt_scheme: db::OptScheme)
               -> PungRpc {

        let mut extra_tuples = Vec::with_capacity(extra);
        let mut rng = ChaChaRng::new_unseeded();

        for _ in 0..extra {
            let mut temp = [0u8; db::TUPLE_SIZE];
            rng.fill_bytes(&mut temp);
            extra_tuples.push(db::PungTuple::new(&temp[..]));
        }

        PungRpc {
            round: 0,
            clients: HashMap::new(),
            worker: worker,
            phase: Phase::Sending,
            send_ctx: SendCtx {
                reqs: HashMap::new(), // gets updated every round
                queue: HashMap::new(),
                handler: send,
                count: 0,
            },
            ret_ctx: RetCtx { reqs: HashMap::new() },
            dbase: dbase,
            extra_tuples: extra_tuples,
            min_messages: min_messages,
            opt_scheme: opt_scheme,
        }
    }

    pub fn max_retries(&self, buckets: usize) -> u32 {
        match self.opt_scheme {
            db::OptScheme::Normal => retry_bound!(buckets),
            db::OptScheme::Aliasing => retry_bound!(buckets, 2),
            db::OptScheme::Hybrid2 => retry_bound!(buckets, 2) / 2,
            db::OptScheme::Hybrid4 => 1,
        }
    }

    pub fn next_id(&self) -> u64 {
        self.clients.len() as u64
    }
}


// Implementation of RPC stubs (see schema/pung.capnp)

impl pung_rpc::Server for PungRpc {
    // TODO: Upgrade this to receive keys for directory service
    fn register(&mut self, params: RegisterParams, mut res: RegisterResults) -> gj::Promise<(), Error> {

        let req = pry!(params.get());
        let rate: u32 = req.get_rate();
        let id: u64 = self.next_id();

        if rate == 0 {
            return gj::Promise::err(Error::failed("Invalid rate (0)".to_string()));
        }

        self.clients.insert(id, rate);
        res.get().set_id(id);
        gj::Promise::ok(())
    }

    // TODO: upgrade to be able to replace directory service key
    fn sync(&mut self, params: SyncParams, mut res: SyncResults) -> gj::Promise<(), Error> {

        let id = pry!(params.get()).get_id();

        if !self.clients.contains_key(&id) {
            return gj::Promise::err(Error::failed("Invalid id during sync".to_string()));
        }

        // If we are already in receive phase, client has to wait for next send phase to begin
        if self.phase == Phase::Receiving {

            res.get().set_round(self.round + 1);

        } else {

            self.send_ctx.reqs.entry(id).or_insert(*self.clients.get(&id).unwrap());
            self.ret_ctx.reqs.entry(id).or_insert(0);
            res.get().set_round(self.round);
        }

        gj::Promise::ok(())
    }


    fn close(&mut self, params: CloseParams, mut res: CloseResults) -> gj::Promise<(), Error> {

        let req = pry!(params.get());
        let id: u64 = req.get_id();

        if !self.clients.contains_key(&id) {
            return gj::Promise::err(Error::failed("Id does not exist".to_string()));
        }

        self.clients.remove(&id);

        if self.send_ctx.reqs.contains_key(&id) {
            self.send_ctx.reqs.remove(&id);
        }

        if self.ret_ctx.reqs.contains_key(&id) {
            self.ret_ctx.reqs.remove(&id);
        }

        res.get().set_success(true);
        gj::Promise::ok(())
    }

    fn change_extra(&mut self, params: ChangeExtraParams, mut res: ChangeExtraResults) -> gj::Promise<(), Error> {

        let req = pry!(params.get());
        let extra: u64 = req.get_extra();

        let mut extra_tuples = Vec::with_capacity(extra as usize);
        let mut rng = ChaChaRng::new_unseeded();

        for _ in 0..extra {
            let mut temp = [0u8; db::TUPLE_SIZE];
            rng.fill_bytes(&mut temp);
            extra_tuples.push(db::PungTuple::new(&temp[..]));
        }

        self.extra_tuples = extra_tuples;

        res.get().set_success(true);
        gj::Promise::ok(())
    }

    fn get_mapping(&mut self, params: GetMappingParams, mut res: GetMappingResults) -> gj::Promise<(), Error> {

        let round = pry!(params.get()).get_round();

        if round != self.round {
            return gj::Promise::err(Error::failed("Invalid round number".to_string()));
        } else if self.phase != Phase::Receiving {
            return gj::Promise::err(Error::failed("Not a receive phase".to_string()));
        }

        let db = self.dbase.borrow();
        // Indices of collections that contain meaningful labels
        let label_collections: Vec<usize> = util::label_collections(self.opt_scheme);

        let mut collection_list = res.get().init_labels((db.num_buckets() * label_collections.len()) as u32);
        let mut collection_idx = 0;

        for bucket in db.get_buckets() {

            for i in &label_collections {

                let collection = bucket.get_collection(*i);
                let mut label_list = collection_list.borrow().init(collection_idx, collection.len() as u32);

                for j in 0..collection.len() {
                    label_list.set(j as u32, collection.get_label(j));
                }

                collection_idx += 1;
            }
        }

        gj::Promise::ok(())
    }

    fn get_bloom(&mut self, params: GetBloomParams, mut res: GetBloomResults) -> gj::Promise<(), Error> {
        let round = pry!(params.get()).get_round();

        if round != self.round {
            return gj::Promise::err(Error::failed("Invalid round number".to_string()));
        } else if self.phase != Phase::Receiving {
            return gj::Promise::err(Error::failed("Not a receive phase".to_string()));
        }

        let db = self.dbase.borrow();

        // Indices of collections that contain meaningful labels
        let label_collections: Vec<usize> = util::label_collections(self.opt_scheme);

        let mut collection_list = res.get().init_blooms((db.num_buckets() * label_collections.len()) as u32);
        let mut collection_idx = 0;

        for bucket in db.get_buckets() {

            for i in &label_collections {
                let collection = bucket.get_collection(*i);
                collection_list.set(collection_idx, &collection.get_bloom().to_bytes());
                collection_idx += 1;
            }
        }

        gj::Promise::ok(())
    }


    fn send(&mut self, params: SendParams, mut res: SendResults) -> gj::Promise<(), Error> {

        let req = pry!(params.get());
        let id: u64 = req.get_id();
        let round: u64 = req.get_round();

        // Ensure client is allowed to send.
        if !self.clients.contains_key(&id) {
            return gj::Promise::err(Error::failed("Invalid id during send.".to_string()));
        } else if round < self.round {
            return gj::Promise::err(Error::failed("Invalid round number.".to_string()));
        } else if self.phase != Phase::Sending && round == self.round {
            return gj::Promise::err(Error::failed("Not sending phase.".to_string()));
        }


        // Create fulfillers so that when we have all info we can respond to clients
        let (promise, fulfiller) = gj::Promise::and_fulfiller();

        {

            // Get tuples
            if !req.has_tuples() {
                return gj::Promise::err(Error::failed("Number of tuples sent is 0".to_string()));
            }

            let tuple_data_list = pry!(req.get_tuples());

            let send_fulfillers = &mut self.send_ctx.handler.fulfillers.borrow_mut();

            if round > self.round {

                // Queue request if round > self.round
                let queue_list = &mut self.send_ctx.queue.entry(round).or_insert_with(Vec::new);

                let mut tuple_list: Vec<db::PungTuple> = Vec::with_capacity(tuple_data_list.len() as usize);

                for i in 0..tuple_data_list.len() {

                    let tuple_data = pry!(tuple_data_list.get(i));
                    let mut offset: usize = 0;

                    // If power of two, clone the tuple under the two provided labels
                    // The format of the message is: (label1, label2, cipher, mac)
                    if self.opt_scheme >= db::OptScheme::Aliasing {

                        offset = db::LABEL_SIZE;
                        let mut tuple_alias_data = Vec::with_capacity(db::TUPLE_SIZE);
                        tuple_alias_data.extend_from_slice(&tuple_data[..offset]);
                        tuple_alias_data.extend_from_slice(&tuple_data[offset * 2..]);

                        tuple_list.push(db::PungTuple::new(&tuple_alias_data[..]));
                    }

                    tuple_list.push(db::PungTuple::new(&tuple_data[offset..]));
                }

                queue_list.push((id, tuple_list, fulfiller));

            } else {

                if !self.send_ctx.reqs.contains_key(&id) {
                    return gj::Promise::err(Error::failed("Client is not synchronized.".to_string()));
                } else if *self.send_ctx.reqs.get(&id).unwrap() < tuple_data_list.len() {
                    return gj::Promise::err(Error::failed("Send rate exceeded.".to_string()));
                }

                if let Some(entry) = self.send_ctx.reqs.get_mut(&id) {
                    *entry -= tuple_data_list.len() as u32;
                }

                for i in 0..tuple_data_list.len() {

                    let tuple_data = pry!(tuple_data_list.get(i));
                    let mut offset: usize = 0;

                    // If power of two, clone the tuple under the two provided labels
                    if self.opt_scheme >= db::OptScheme::Aliasing {

                        offset = db::LABEL_SIZE;
                        let mut tuple_alias_data = Vec::with_capacity(db::TUPLE_SIZE);
                        tuple_alias_data.extend_from_slice(&tuple_data[..offset]);
                        tuple_alias_data.extend_from_slice(&tuple_data[offset * 2..]);

                        let tuple_alias = db::PungTuple::new(&tuple_alias_data[..]);

                        self.send_ctx.count += 1;
                        self.send_ctx.handler.input.send(tuple_alias);
                    }

                    let tuple = db::PungTuple::new(&tuple_data[offset..]);

                    self.send_ctx.count += 1;
                    self.send_ctx.handler.input.send(tuple);
                }

                send_fulfillers.push(fulfiller);
            }


            // Push any queued requests for the current round
            if let Some(mut queued) = self.send_ctx.queue.remove(&self.round) {

                for (cid, mut tuple_list, f) in queued.drain(..) {

                    let alias = if self.opt_scheme >= db::OptScheme::Aliasing {
                        2
                    } else {
                        1
                    };

                    // Check if queued request is valid, if not, reject it
                    if !self.send_ctx.reqs.contains_key(&cid) {
                        f.reject(Error::failed("Client is not synchronized.".to_string()));
                    } else if *self.send_ctx.reqs.get(&cid).unwrap() * alias < tuple_list.len() as u32 {
                        f.reject(Error::failed("Send rate exceeded (queue).".to_string()));
                    } else {

                        // if valid, process it as if it had been sent this round

                        if let Some(entry) = self.send_ctx.reqs.get_mut(&cid) {
                            *entry -= tuple_list.len() as u32 / alias;
                        }

                        for t in tuple_list.drain(..) {
                            self.send_ctx.count += 1;
                            self.send_ctx.handler.input.send(t);
                        }

                        send_fulfillers.push(f);
                    }
                }
            }
        }

        let opt_scheme = self.opt_scheme;

        // promise returned to the client (when we have all tuples we can return this info)
        let ret_promise = promise.then(move |ret: Rc<(Vec<u64>, Vec<Vec<u8>>)>| {

            {
                let mut num_list = res.get().init_num_messages(ret.0.len() as u32);

                for i in 0..ret.0.len() {
                    num_list.set(i as u32, ret.0[i]);
                }
            }

            if opt_scheme >= db::OptScheme::Hybrid2 {

                let mut lmid_list = res.get().init_min_labels(ret.1.len() as u32);
                for i in 0..ret.1.len() {
                    lmid_list.set(i as u32, &(ret.1[i])[..]);
                }
            }

            gj::Promise::ok(())
        });

        // TODO: not sure if this has any effect...
        //    self.worker.step();

        // TODO: maybe add timeout? Right now it waits for all clients to send.

        // Check to see if all clients have sent all their tuples
        if !self.send_ctx.reqs.values().any(|&x| x > 0) && self.phase == Phase::Sending &&
           self.send_ctx.count >= self.min_messages {

            for t in &self.extra_tuples {
                self.send_ctx.handler.input.send(t.clone());
            }

            self.send_ctx.handler.input.advance_to(self.round as usize + 1);

            while self.send_ctx.handler.probe.le(&RootTimestamp::new(self.round as usize)) {
                self.worker.step();
            }


            let db = self.dbase.borrow();

            let total_dbs = db.total_dbs() as u32;
            let retries = self.max_retries(db.num_buckets());

            // Update the number of expected retrievals per client.
            for (_, v) in &mut self.ret_ctx.reqs {
                *v = total_dbs * retries;
            }

            self.phase = Phase::Receiving;
        }

        ret_promise
    }


    fn retr(&mut self, params: RetrParams, mut res: RetrResults) -> gj::Promise<(), Error> {

        let req = pry!(params.get());
        let id: u64 = req.get_id();
        let round: u64 = req.get_round();

        if !self.clients.contains_key(&id) {
            return gj::Promise::err(Error::failed("Invalid id during send.".to_string()));
        } else if round != self.round {
            return gj::Promise::err(Error::failed("Invalid round number".to_string()));
        } else if self.phase != Phase::Receiving {
            return gj::Promise::err(Error::failed("Invalid phase for retrieval".to_string()));
        } else if !self.ret_ctx.reqs.contains_key(&id) {
            return gj::Promise::err(Error::failed("(ret) Client is not synchronized.".to_string()));
        } else if *self.ret_ctx.reqs.get(&id).unwrap() == 0 {
            return gj::Promise::err(Error::failed("retrieveal rate exceeded.".to_string()));
        }

        let bucket_idx: usize = req.get_bucket() as usize;
        let collection_idx: usize = req.get_collection() as usize;
        let level_idx: usize = req.get_level() as usize;
        let query: &[u8] = pry!(req.get_query());
        let q_num: u64 = req.get_qnum();

        // check to make sure level
        let mut db = self.dbase.borrow_mut();

        if bucket_idx >= db.num_buckets() {
            return gj::Promise::err(Error::failed("invalid bucket requested".to_string()));
        }

        // Process this bucket
        {
            let bucket = db.get_bucket(bucket_idx);

            if collection_idx >= bucket.num_collections() {
                return gj::Promise::err(Error::failed("invalid collection requested".to_string()));
            }

            let collection = bucket.get_collection(collection_idx);

            if level_idx >= collection.num_levels() {
                return gj::Promise::err(Error::failed("invalid level requested".to_string()));
            }

            // let start = time::PreciseTime::now();
            {
                let pir_handler = collection.pir_handler(level_idx);

                let answer = pir_handler.gen_answer(query, q_num);
                res.get().set_answer(answer.answer);
                res.get().set_anum(answer.num);
            }
            // let end = time::PreciseTime::now();
            // println!("bucket {}, collection {}, level {}, answer time: {} usec",
            //         bucket_idx, collection_idx, level_idx, start.to(end).num_microseconds().unwrap());

        }

        // Account for this retrieval
        if let Some(entry) = self.ret_ctx.reqs.get_mut(&id) {
            *entry -= 1;
        }

        // Check to see if we are done and we can move on to next round
        if !self.ret_ctx.reqs.values().any(|&x| x > 0) {

            self.send_ctx.reqs = self.clients.clone();
            self.send_ctx.count = 0;
            self.round += 1;
            self.phase = Phase::Sending;
            db.clear(); // Garbage collect the whole thing

            println!("Advancing to round {}", self.round);
        }

        gj::Promise::ok(())
    }
}
