// This file contains all the sending and retrieving routines
// that can be used by a client application (see src/bin/client.rs) to
// create a Pung client. All the client-side cryptographic operations 
// (not related to PIR) are found in pcrypto.rs.

// serialization libraries
use capnp;
use capnp::Error;
use capnp_rpc::{RpcSystem, rpc_twoparty_capnp, twoparty};

use db;
use gj; 
use gjio; // asynchronous IO libraries

use pir::pir_client::PirClient;
use pung_capnp::pung_rpc;

use rand;
use rand::Rng;
use std::cmp::Ordering;
use std::collections::HashMap;
use std::collections::HashSet;
use std::net::ToSocketAddrs;

use util;
use util::bloomfilter;

pub mod pcrypto;

struct PungPeer {
    uid_self: u64,
    uid_peer: u64,
    keys: pcrypto::PungKeys,
}

impl PungPeer {
    pub fn new(uid_self: u64, uid_peer: u64, keys: pcrypto::PungKeys) -> PungPeer {
        PungPeer { uid_self: uid_self, uid_peer: uid_peer, keys: keys }
    }
}

// information about a bucket. Number of tuples in the bucket, and lmid
struct BucketInfo {
    num: u64,
    lmid: Vec<Vec<u8>>,
}

impl BucketInfo {
    pub fn num_tuples(&self) -> u64 {
        self.num
    }

    pub fn get_lmid(&self, idx: usize) -> &[u8] {
        &self.lmid[idx][..]
    }

    pub fn get_lmids(&self) -> &[Vec<u8>] {
        &self.lmid[..]
    }
}

pub struct PungClient<'a> {
    id: u64, // id to register with service
    name: &'a str,
    send_rate: u32,
    ret_rate: u32, // roughly same as # of buckets

    conn: pung_rpc::Client,

    round: u64,
    buckets: Vec<BucketInfo>, // Information about buckets for this round

    ret_scheme: db::RetScheme, // retrieval scheme
    opt_scheme: db::OptScheme, // optimization scheme

    peers: HashMap<&'a str, PungPeer>,

    pir_handler: PirClient<'a>,
    partitions: Vec<Vec<u8>>, // Static partitioning of label space

    // Mapping between collection and encoding recipe (i.e., which pieces to xor together)
    h4_mappings: HashMap<usize, [HashSet<usize>; 4]>,
}


macro_rules! h_set {
    ($x:expr) => ($x.iter().cloned().collect())
}


impl<'a> PungClient<'a> {
    pub fn new(name: &'a str,
               address: &str,
               send_rate: u32,
               ret_rate: u32,
               depth: u64,
               ret_scheme: db::RetScheme,
               opt_scheme: db::OptScheme,
               scope: &gj::WaitScope,
               port: &mut gjio::EventPort)
               -> PungClient<'a> {

        let addr = match address.to_socket_addrs() {
            Ok(mut v) => {
                match v.next() {
                    Some(a) => a,
                    None => panic!("Error: Address iterator is empty."),
                }
            }

            Err(e) => panic!("Error parsing address: {:?}", e),
        };

        let network = port.get_network();

        let address = network.get_tcp_address(addr);
        let stream = match address.connect().wait(scope, port) {
            Ok(s) => s,
            Err(e) => panic!("Error connecting to addr: {:?}", e),
        };

        let mut reader_options: capnp::message::ReaderOptions = Default::default();
        reader_options.traversal_limit_in_words(300 * 1024 * 1024);

        let network = Box::new(twoparty::VatNetwork::new(stream.clone(),
                                                         stream,
                                                         rpc_twoparty_capnp::Side::Client,
                                                         Default::default()));

        // Initialize RPC client
        let mut rpc_system = RpcSystem::new(network, None);

        // Initialize static partitions of label space
        let mut partitions: Vec<Vec<u8>> = Vec::with_capacity(ret_rate as usize);

        for i in 0..ret_rate as usize {
            partitions.push(util::label_marker(i, ret_rate as usize));
        }

        // Initialize h4 mapping
        let mut h4_mappings = HashMap::new();

        if opt_scheme == db::OptScheme::Hybrid4 {
            // The following are parts with which to build the collection
            // For example, collection 0 can be built using 0, 1 XOR 4, 2 XOR 6, or the rest.
            h4_mappings.insert(0, [h_set!([0]), h_set!([1, 4]), h_set!([2, 6]), h_set!([3, 5, 7, 8])]);
            h4_mappings.insert(1, [h_set!([1]), h_set!([0, 4]), h_set!([3, 7]), h_set!([2, 5, 6, 8])]);
            h4_mappings.insert(2, [h_set!([2]), h_set!([3, 5]), h_set!([0, 6]), h_set!([1, 4, 7, 8])]);
            h4_mappings.insert(3, [h_set!([3]), h_set!([2, 5]), h_set!([1, 7]), h_set!([0, 4, 6, 8])]);
        }

        PungClient {
            id: 0,
            name: name,
            send_rate: send_rate,
            ret_rate: ret_rate,
            round: 0,
            buckets: Vec::with_capacity(ret_rate as usize),
            conn: rpc_system.bootstrap(rpc_twoparty_capnp::Side::Server),
            ret_scheme: ret_scheme,
            opt_scheme: opt_scheme,
            peers: HashMap::new(),
            pir_handler: PirClient::new(1, 1, 1, depth),
            partitions: partitions,
            h4_mappings: h4_mappings,
        }
    }

    pub fn get_round(&self) -> u64 {
        self.round
    }

    pub fn inc_round(&mut self, val: u64) {
        self.round += val;
        self.buckets.clear();
    }

    /// Adds a peer. A unique id between peer and `self` is derived
    /// based on the names (lexicographically smaller name gets 0,
    /// the other gets 1).
    pub fn add_peer(&mut self, peer: &'a str, secret: &[u8]) {
        let keys = pcrypto::derive_keys(secret);

        if self.name < peer {
            self.peers.insert(peer, PungPeer::new(0, 1, keys));
        } else if self.name > peer {
            self.peers.insert(peer, PungPeer::new(1, 0, keys));
        } else {
            self.peers.insert(peer, PungPeer::new(0, 0, keys));
        }
    }

    /// Sets up a fake peer with which to encrypt messages that are meant to be sent to nobody
    pub fn init_dummy_peer(&mut self) {
        let mut secret = [0u8; 256];
        let mut rng = rand::ChaChaRng::new_unseeded();
        rng.fill_bytes(&mut secret);

        let keys = pcrypto::derive_keys(&secret);
        self.peers.insert("dummy", PungPeer::new(0, 0, keys));
    }

    /// Register with the server and receive a client id
    pub fn register(&mut self, scope: &gj::WaitScope, port: &mut gjio::EventPort) -> Result<u64, Error> {

        let mut reg_request = self.conn.register_request();
        reg_request.get().set_rate(self.send_rate);

        let response = try!(reg_request.send().promise.wait(scope, port));
        let id: u64 = try!(response.get()).get_id();

        self.id = id;
        Ok(id)
    }


    // This is just to make testing and data collection easier
    pub fn extra(&self, extra: u64, scope: &gj::WaitScope, port: &mut gjio::EventPort) -> Result<(), Error> {

        let mut extra_request = self.conn.change_extra_request();
        extra_request.get().set_extra(extra);

        let response = try!(extra_request.send().promise.wait(scope, port));

        if try!(response.get()).get_success() {
            Ok(())
        } else {
            Err(Error::failed("Failed to change extra tuples.".to_string()))
        }
    }

    /// End connection with the server.
    pub fn close(&self, scope: &gj::WaitScope, port: &mut gjio::EventPort) -> Result<(), Error> {

        let mut close_request = self.conn.close_request();
        close_request.get().set_id(self.id);

        let response = try!(close_request.send().promise.wait(scope, port));
        let success = try!(response.get()).get_success();

        if success {
            Ok(())
        } else {
            Err(Error::failed("Failed to unregister.".to_string()))
        }
    }


    /// Sync with server to obtain next available round number
    pub fn sync(&mut self, scope: &gj::WaitScope, port: &mut gjio::EventPort) -> Result<(), Error> {

        let mut sync_request = self.conn.sync_request();
        sync_request.get().set_id(self.id);

        let response = try!(sync_request.send().promise.wait(scope, port));
        let new_round = try!(response.get()).get_round();

        if self.round <= new_round {
            self.round = new_round;
            Ok(())
        } else {
            Err(Error::failed("Invalid round number returned by server".to_string()))
        }
    }

    fn max_retries(&self) -> u32 {
        match self.opt_scheme {
            db::OptScheme::Normal => retry_bound!(self.ret_rate),
            db::OptScheme::Aliasing => retry_bound!(self.ret_rate, 2),
            db::OptScheme::Hybrid2 => retry_bound!(self.ret_rate, 2) / 2,
            db::OptScheme::Hybrid4 => 1,
        }
    }

    /// Send a tuple (or set of tuples) to the server
    pub fn send(&mut self,
                recipient: &str,
                msgs: &mut Vec<Vec<u8>>,
                scope: &gj::WaitScope,
                port: &mut gjio::EventPort)
                -> Result<u64, Error> {

        if !self.peers.contains_key(&recipient) {
            return Err(Error::failed("Invalid recipient name".to_string()));
        } else if msgs.is_empty() {
            return Err(Error::failed("No messages were provided".to_string()));
        }

        let peer = &self.peers[recipient];
        let mut send_request = self.conn.send_request();
        send_request.get().set_id(self.id);
        send_request.get().set_round(self.round);

        {
            let mut tuple_list = send_request.get().init_tuples(msgs.len() as u32);
            let mut idx: u32 = 0;
            let mut measurement_byte_count = 0;

            for msg in msgs.drain(..) {

                let (mut c, mut mac) = pcrypto::encrypt(&peer.keys.k_e[..], self.round, &msg[..]);

                let mut tuple = pcrypto::gen_label(&peer.keys.k_l[..], self.round, peer.uid_peer, idx as u64, 0);

                // If we are using aliasing, generate an extra label
                // and make sure it falls in a separate bucket
                if self.opt_scheme >= db::OptScheme::Aliasing {

                    let bucket_idx = util::bucket_idx(&tuple, &self.partitions);

                    let mut label_alias =
                        pcrypto::gen_label(&peer.keys.k_l2[..], self.round, peer.uid_peer, idx as u64, 0);

                    let mut bucket_alias_idx = util::bucket_idx(&label_alias, &self.partitions);

                    let mut collision_count = 1; // count collisions of labels to the same bucket

                    while bucket_idx == bucket_alias_idx {
                        label_alias = pcrypto::gen_label(&peer.keys.k_l2[..],
                                                         self.round,
                                                         peer.uid_peer,
                                                         idx as u64,
                                                         collision_count);

                        bucket_alias_idx = util::bucket_idx(&label_alias, &self.partitions);
                        collision_count += 1;
                    }

                    // Postcondtion: the two labels fall in different buckets

                    tuple.append(&mut label_alias);
                }

                tuple.append(&mut c);
                tuple.append(&mut mac);

                measurement_byte_count += tuple.len();

                tuple_list.set(idx as u32, &tuple[..]);
                idx += 1;
            }

            println!("Upload (send rpc) {} bytes", measurement_byte_count + 16);
        }

        // get RPC response which contains total number of tuples and lmids

        let mut total_tuples: u64 = 0;

        let res_ptr = try!(send_request.send().promise.wait(scope, port));
        let response = try!(res_ptr.get());

        let buckets_num = try!(response.get_num_messages());
        assert_eq!(buckets_num.len(), self.ret_rate);

        if self.opt_scheme == db::OptScheme::Hybrid2 {

            let buckets_lmid = try!(response.get_min_labels());
            assert_eq!(buckets_num.len(), buckets_lmid.len());

            for i in 0..buckets_num.len() {

                self.buckets
                    .push(BucketInfo { num: buckets_num.get(i), lmid: vec![try!(buckets_lmid.get(i)).to_vec()] });

                total_tuples += buckets_num.get(i);
            }

            // This accounts for: 8 bytes (64 bits) for each bucket number entry
            // and the Lmid label
            println!("Download (send rpc) {} bytes",
                     (buckets_num.len() * 8) + (buckets_lmid.len() * db::LABEL_SIZE as u32));

        } else if self.opt_scheme == db::OptScheme::Hybrid4 {

            let buckets_lmid = try!(response.get_min_labels());
            assert_eq!(buckets_num.len() * 3, buckets_lmid.len()); // delimeters per bucket

            for i in 0..buckets_num.len() {

                let mut lmid = Vec::with_capacity(3);

                for j in 0..3 {
                    // collections
                    lmid.push(try!(buckets_lmid.get(3 * i + j)).to_vec());
                }

                self.buckets.push(BucketInfo { num: buckets_num.get(i), lmid: lmid });
                total_tuples += buckets_num.get(i);
            }

            // This accounts for: 8 bytes (64 bits) for each bucket number entry
            // and the 3 Lmid labels per bucket
            println!("Download (send rpc) {} bytes",
                     (buckets_num.len() * 8) + (buckets_lmid.len() * db::LABEL_SIZE as u32));

        } else {

            for i in 0..buckets_num.len() {
                self.buckets.push(BucketInfo { num: buckets_num.get(i), lmid: Vec::new() });
                total_tuples += buckets_num.get(i);
            }

            // 8 bytes (64 bits) for each bucket number entry
            println!("Download (send rpc) {} bytes", buckets_num.len() * 8);
        }

        Ok(total_tuples)
    }

    // Given a list of peers from whom to retrieve a message, derive the label(s) and build
    // a list of labels for each bucket. Output maps from bucket to list of (peer, label).
    // Peer object is needed to decrypt file once it has been retrieved.
    fn schedule(&'a self, peer_names: &[&'a str]) -> Result<HashMap<usize, Vec<(&'a PungPeer, Vec<u8>)>>, Error> {

        // bucket_id -> [(peer, label)]
        let mut bucket_map: HashMap<usize, Vec<(&'a PungPeer, Vec<u8>)>> = HashMap::new();
        // maps from peer name to which message this is (first, second, third, etc.)
        let mut peer_count: HashMap<&str, u64> = HashMap::new();

        // Go through each peer, get labels and see to which bucket they map
        for peer_name in peer_names {

            if !self.peers.contains_key(peer_name) {
                return Err(Error::failed("Invalid peer name".to_string()));
            }

            // get peer object for this sender
            let peer = &self.peers[peer_name];

            // get current count for this peer (in case of repeated messages)
            let count = peer_count.entry(peer_name).or_insert(0);

            // get mailbox label for this peer/count
            let label = pcrypto::gen_label(&peer.keys.k_l[..], self.round, peer.uid_self, *count, 0);

            // find out on which bucket this label falls
            let bucket_idx = util::bucket_idx(&label, &self.partitions);

            // Add (peer, label) to the bucket map. If there are collisions, append it to list
            // If there is aliasing, derive second label too

            if self.opt_scheme >= db::OptScheme::Aliasing {


                let mut collisions = 0; // Number of collisions found so far
                let mut label_alias =
                    pcrypto::gen_label(&peer.keys.k_l2[..], self.round, peer.uid_self, *count, collisions);
                let mut bucket_idx_alias = util::bucket_idx(&label_alias, &self.partitions);

                // Derive a different label if there are collisions (must ensure labels map to
                // different buckets)
                while bucket_idx == bucket_idx_alias {
                    collisions += 1;
                    label_alias =
                        pcrypto::gen_label(&peer.keys.k_l2[..], self.round, peer.uid_self, *count, collisions);
                    bucket_idx_alias = util::bucket_idx(&label_alias, &self.partitions);
                }

                // Lenghts of the buckets
                let len1 = if let Some(bucket) = bucket_map.get(&bucket_idx) {
                    bucket.len()
                } else {
                    0
                };

                let len2 = if let Some(bucket) = bucket_map.get(&bucket_idx_alias) {
                    bucket.len()
                } else {
                    0
                };

                // Add label to the least full bucket
                if len1 < len2 {
                    let bucket_entry = bucket_map.entry(bucket_idx).or_insert_with(Vec::new);
                    bucket_entry.push((peer, label));
                } else {
                    let bucket_entry = bucket_map.entry(bucket_idx_alias).or_insert_with(Vec::new);
                    bucket_entry.push((peer, label_alias));
                }
            } else {
                let bucket_entry = bucket_map.entry(bucket_idx).or_insert_with(Vec::new);
                bucket_entry.push((peer, label));
            }

            *count += 1;  // update # messages from this peer
        }

        Ok(bucket_map)
    }


    fn next_label(&'a self,
                  bucket_map: &mut HashMap<usize, Vec<(&'a PungPeer, Vec<u8>)>>,
                  bucket: usize,
                  dummy: &'a PungPeer,
                  dummy_count: &mut u64)
                  -> (&'a PungPeer, Vec<u8>) {

        match bucket_map.remove(&bucket) {
            Some(mut v) => {
                // this is a vector of (peer, label)
                let t = v.pop().unwrap();

                // re-insert vector if there are any labels left
                if !v.is_empty() {
                    bucket_map.insert(bucket, v);
                }

                t
            }

            None => {
                // Request for this bucket will have to be a dummy one
                let label = pcrypto::gen_label(&dummy.keys.k_l[..], self.round, dummy.uid_self, *dummy_count, 0);
                *dummy_count += 1;
                (dummy, label)
            }
        }
    }

    // Returns a map of bucket -> (collection -> [labels])
    fn get_explicit_labels(&self,
                           scope: &gj::WaitScope,
                           port: &mut gjio::EventPort)
                           -> Result<HashMap<usize, HashMap<usize, Vec<Vec<u8>>>>, Error> {

        let mut map_request = self.conn.get_mapping_request();
        map_request.get().set_round(self.round);

        // RPC is 8 bytes
        println!("Upload (explicit label rpc) {} bytes", 8);

        let response = try!(map_request.send().promise.wait(scope, port));

        if !try!(response.get()).has_labels() {
            return Err(Error::failed("Empty label mapping returned by server".to_string()));
        }

        // This is a list(list(label)) = list(list([u8]))
        let collection_list = try!(try!(response.get()).get_labels());
        let mut response_idx = 0;

        // index of collection(s) within a bucket containing meaningful labels
        let meaningful_labels: Vec<usize> = util::label_collections(self.opt_scheme);

        let mut label_map: HashMap<usize, HashMap<usize, Vec<Vec<u8>>>> = HashMap::new();

        let mut download_measurement = 0;

        for bucket_idx in 0..self.buckets.len() {

            let mut bucket_map = label_map.entry(bucket_idx).or_insert_with(HashMap::new);

            for collection_idx in &meaningful_labels {

                let mut collection_vec = bucket_map.entry(*collection_idx).or_insert_with(Vec::new);

                // This is the returned list(label) = list([u8])
                let label_list = try!(collection_list.get(response_idx));

                for i in 0..label_list.len() {
                    collection_vec.push(label_list.get(i).unwrap().to_vec());
                    download_measurement += db::LABEL_SIZE;
                }

                response_idx += 1;
            }
        }

        println!("Download (explicit label rpc) {} bytes", download_measurement);

        Ok(label_map)
    }


    // Returns a bloom filter that encodes the labels
    fn get_bloom_filter(&self,
                        scope: &gj::WaitScope,
                        port: &mut gjio::EventPort)
                        -> Result<HashMap<usize, HashMap<usize, bloomfilter::Bloom>>, Error> {

        let mut bloom_request = self.conn.get_bloom_request();
        bloom_request.get().set_round(self.round);

        // RPC is 8 bytes
        println!("Upload (bloom filter rpc) {} bytes", 8);

        let response = try!(bloom_request.send().promise.wait(scope, port));

        if !try!(response.get()).has_blooms() {
            return Err(Error::failed("Empty bloom map returned by server".to_string()));
        }

        // This is a list(bit_vec)
        let bit_vec_list = try!(try!(response.get()).get_blooms());

        let mut response_idx = 0;

        // index of collection(s) within a bucket containing meaningful labels
        let meaningful_labels: Vec<usize> = util::label_collections(self.opt_scheme);

        let mut bloom_map: HashMap<usize, HashMap<usize, bloomfilter::Bloom>> = HashMap::new();

        let mut download_measurement = 0;

        for bucket_idx in 0..self.buckets.len() {

            let mut bucket_map = bloom_map.entry(bucket_idx).or_insert_with(HashMap::new);
            let num_tuples = self.buckets[bucket_idx].num_tuples();


            for collection_idx in &meaningful_labels {

                // Number of tuples in collection
                let t_num =
                    util::collection_len(num_tuples, *collection_idx as u32, meaningful_labels.len() as u32);

                // This is the returned bit_vec
                let bit_vec = try!(bit_vec_list.get(response_idx));

                download_measurement += bit_vec.len();

                // Create a bloom filter from bit vector
                let mut bloom = bloomfilter::Bloom::new_for_fp_rate(t_num as usize, db::BLOOM_FP);
                bloom.from_bytes(bit_vec);

                // Insert bloom filter
                bucket_map.insert(*collection_idx, bloom);

                response_idx += 1;
            }
        }

        println!("Download (bloom filter rpc) {} bytes", download_measurement);

        Ok(bloom_map)
    }

    // Retrieves a message (or set of messages) form the server based on bucket_map
    fn retr_normal(&'a self,
                   mut bucket_map: HashMap<usize, Vec<(&'a PungPeer, Vec<u8>)>>,
                   scope: &gj::WaitScope,
                   port: &mut gjio::EventPort)
                   -> Result<Vec<Vec<u8>>, Error> {

        let retries = self.max_retries();
        let dummy = &self.peers["dummy"];
        let mut dummy_count = 0;
        let mut rng = rand::ChaChaRng::new_unseeded();
        let mut messages: Vec<Vec<u8>> = Vec::new();

        match self.ret_scheme {

            db::RetScheme::Explicit => {

                // Get labels explicitly
                let explicit_labels = try!(self.get_explicit_labels(scope, port));

                for _ in 0..retries {
                    for bucket in 0..self.partitions.len() {

                        // Get next label to retrieve
                        let (peer, label) = self.next_label(&mut bucket_map, bucket, dummy, &mut dummy_count);

                        // Number of elements in bucket
                        let num = self.buckets[bucket].num_tuples();

                        // Get labels of collection 0 (which is the entire bucket)
                        let labels = &explicit_labels[&bucket][&0];
                        assert_eq!(num, labels.len() as u64);

                        // Get index of label if available or random otherwise
                        let idx = some_or_random!(util::get_index(labels, &label), rng, num);

                        // Get a tuple using PIR to retrieve
                        let t = try!(self.pir_retr(bucket, 0, 0, idx, num, scope, port));

                        if t.label() == &label[..] {
                            // decrypt ciphertext using shared key and insert it into message list
                            let m = try!(pcrypto::decrypt(&peer.keys.k_e[..], self.round, t.cipher(), t.mac()));
                            messages.push(m);
                        }
                    }
                }
            }

            db::RetScheme::Bloom => {

                // Get bloom filter
                let bloom_filters = try!(self.get_bloom_filter(scope, port));

                for _ in 0..retries {
                    for bucket in 0..self.partitions.len() {

                        // Get next label to retrieve
                        let (peer, label) = self.next_label(&mut bucket_map, bucket, dummy, &mut dummy_count);

                        // Number of elements in bucket
                        let num = self.buckets[bucket].num_tuples();

                        // Get bloom filter of collection 0 (entire bucket)
                        let bloom = &bloom_filters[&bucket][&0];

                        // Get index of label if available or random otherwise
                        let idx = some_or_random!(util::get_idx_bloom(bloom, &label, num), rng, num);

                        // Get a tuple using PIR to retrieve
                        let t = try!(self.pir_retr(bucket, 0, 0, idx, num, scope, port));

                        if t.label() == &label[..] {
                            // decrypt ciphertext using shared key and insert it into message list
                            let m = try!(pcrypto::decrypt(&peer.keys.k_e[..], self.round, t.cipher(), t.mac()));
                            messages.push(m);
                        }
                    }
                }
            }

            db::RetScheme::Tree => {

                for _ in 0..retries {
                    for bucket in 0..self.partitions.len() {

                        // Get next label
                        let (peer, label) = self.next_label(&mut bucket_map, bucket, dummy, &mut dummy_count);

                        // Number of elemnets in bucket
                        let num = self.buckets[bucket].num_tuples();

                        // Perform bst retrieval
                        let result = try!(self.bst_retr(&label[..], bucket, 0, num, &mut rng, scope, port));

                        if let Some(t) = result {
                            // decrypt ciphertext using shared key and insert it into message list
                            let m = try!(pcrypto::decrypt(&peer.keys.k_e[..], self.round, t.cipher(), t.mac()));
                            messages.push(m);
                        }
                    }
                }
            }
        }

        Ok(messages)
    }


    fn retr_hybrid2(&'a self,
                    mut bucket_map: HashMap<usize, Vec<(&'a PungPeer, Vec<u8>)>>,
                    scope: &gj::WaitScope,
                    port: &mut gjio::EventPort)
                    -> Result<Vec<Vec<u8>>, Error> {

        let retries = self.max_retries();
        let dummy = &self.peers["dummy"];
        let mut dummy_count = 0;
        let mut rng = rand::ChaChaRng::new_unseeded();
        let mut messages: Vec<Vec<u8>> = Vec::new();


        match self.ret_scheme {

            db::RetScheme::Explicit => {

                // Get labels explicitly
                let explicit_labels = try!(self.get_explicit_labels(scope, port));

                for _ in 0..retries {
                    for bucket in 0..self.partitions.len() {

                        // Get 2 labels to retrieve
                        let (peer1, label1) = self.next_label(&mut bucket_map, bucket, dummy, &mut dummy_count);
                        let (peer2, label2) = self.next_label(&mut bucket_map, bucket, dummy, &mut dummy_count);

                        let num = self.buckets[bucket].num_tuples();
                        let lmid = self.buckets[bucket].get_lmid(0);

                        // Compare chosen labels to the bucket's lmid
                        let cmp1 = util::label_cmp(&label1[..], lmid);
                        let cmp2 = util::label_cmp(&label2[..], lmid);

                        // Get explicit labels for collections 0 and 1
                        let col0 = &explicit_labels[&bucket][&0];
                        let col1 = &explicit_labels[&bucket][&1];

                        // number of elements in collections 0 and 2
                        let len0 = util::collection_len(num, 0, 2) as u64;
                        // number of elements in collections 1
                        let len1 = util::collection_len(num, 1, 2) as u64;

                        assert_eq!(col0.len() as u64, len0);
                        assert_eq!(col1.len() as u64, len1);
                        assert!(len0 >= len1);

                        // "_" stands for "greater than or equal" in this case
                        let (t1, t2) = match (cmp1, cmp2) {

                            // Case 1: both labels fall in collection 0
                            (Ordering::Less, Ordering::Less) => {
                                let idx1 = some_or_random!(util::get_index(col0, &label1), rng, len0);
                                let idx2 = some_or_random!(util::get_index(col0, &label2), rng, len0);

                                let t1 = try!(self.pir_retr(bucket, 0, 0, idx1, len0, scope, port));
                                let t2 = try!(self.pir_retr(bucket, 1, 0, idx2, len1, scope, port));
                                let t3 = try!(self.pir_retr(bucket, 2, 0, idx2, len0, scope, port));

                                (t1, (&t2 ^ &t3))
                            }

                            // Case 2: label 1 is in collection 0, and label 2 in collection 1
                            (Ordering::Less, _) => {
                                let idx1 = some_or_random!(util::get_index(col0, &label1), rng, len0);
                                let idx2 = some_or_random!(util::get_index(col1, &label2), rng, len1);

                                let t1 = try!(self.pir_retr(bucket, 0, 0, idx1, len0, scope, port));
                                let t2 = try!(self.pir_retr(bucket, 1, 0, idx2, len1, scope, port));

                                // fake request
                                try!(self.pir_retr(bucket, 2, 0, rng.next_u64() % len0, len0, scope, port));

                                (t1, t2)
                            }

                            // Case 3: label 1 is in collection 1, and label 2 in collection 0
                            (_, Ordering::Less) => {
                                let idx1 = some_or_random!(util::get_index(col1, &label1), rng, len1);
                                let idx2 = some_or_random!(util::get_index(col0, &label2), rng, len0);

                                let t2 = try!(self.pir_retr(bucket, 0, 0, idx2, len0, scope, port));
                                let t1 = try!(self.pir_retr(bucket, 1, 0, idx1, len1, scope, port));

                                // fake request
                                try!(self.pir_retr(bucket, 2, 0, rng.next_u64() % len0, len0, scope, port));

                                (t1, t2)
                            }

                            // Case 4: both labels fall in collection 1
                            (_, _) => {
                                let idx1 = some_or_random!(util::get_index(col1, &label1), rng, len1);
                                let idx2 = some_or_random!(util::get_index(col1, &label2), rng, len1);

                                let t1 = try!(self.pir_retr(bucket, 0, 0, idx1, len0, scope, port));
                                let t2 = try!(self.pir_retr(bucket, 1, 0, idx2, len1, scope, port));
                                let t3 = try!(self.pir_retr(bucket, 2, 0, idx1, len0, scope, port));

                                ((&t1 ^ &t3), t2)
                            }
                        };

                        if t1.label() == &label1[..] {
                            // decrypt ciphertext using shared key and insert it into message list
                            let m = try!(pcrypto::decrypt(&peer1.keys.k_e[..], self.round, t1.cipher(), t1.mac()));
                            messages.push(m);
                        }

                        if t2.label() == &label2[..] {
                            // decrypt ciphertext using shared key and insert it into message list
                            let m = try!(pcrypto::decrypt(&peer2.keys.k_e[..], self.round, t2.cipher(), t2.mac()));
                            messages.push(m);
                        }
                    }
                }
            }

            db::RetScheme::Bloom => {

                // Get bloom filters
                let bloom_filters = try!(self.get_bloom_filter(scope, port));

                for _ in 0..retries {
                    for bucket in 0..self.partitions.len() {

                        // Get 2 labels to retrieve
                        let (peer1, label1) = self.next_label(&mut bucket_map, bucket, dummy, &mut dummy_count);
                        let (peer2, label2) = self.next_label(&mut bucket_map, bucket, dummy, &mut dummy_count);

                        let num = self.buckets[bucket].num_tuples();
                        let lmid = self.buckets[bucket].get_lmid(0);

                        // Compare chosen labels to the bucket's lmid
                        let cmp1 = util::label_cmp(&label1[..], lmid);
                        let cmp2 = util::label_cmp(&label2[..], lmid);

                        // Get bloom filter for collections 0 and 1
                        let b0 = &bloom_filters[&bucket][&0];
                        let b1 = &bloom_filters[&bucket][&1];

                        // number of elements in collections 0 and 2
                        let len0 = util::collection_len(num, 0, 2) as u64;
                        // number of elements in collections 1
                        let len1 = util::collection_len(num, 1, 2) as u64;
                        assert!(len0 >= len1);

                        // "_" stands for "greater than or equal" in this case
                        let (t1, t2) = match (cmp1, cmp2) {

                            // Case 1: both labels fall in collection 0
                            (Ordering::Less, Ordering::Less) => {
                                let idx1 = some_or_random!(util::get_idx_bloom(b0, &label1, len0), rng, len0);
                                let idx2 = some_or_random!(util::get_idx_bloom(b0, &label2, len0), rng, len0);

                                let t1 = try!(self.pir_retr(bucket, 0, 0, idx1, len0, scope, port));
                                let t2 = try!(self.pir_retr(bucket, 1, 0, idx2, len1, scope, port));
                                let t3 = try!(self.pir_retr(bucket, 2, 0, idx2, len0, scope, port));

                                (t1, (&t2 ^ &t3))
                            }

                            // Case 2: label 1 is in collection 0, and label 2 in collection 1
                            (Ordering::Less, _) => {
                                let idx1 = some_or_random!(util::get_idx_bloom(b0, &label1, len0), rng, len0);
                                let idx2 = some_or_random!(util::get_idx_bloom(b1, &label2, len1), rng, len1);

                                let t1 = try!(self.pir_retr(bucket, 0, 0, idx1, len0, scope, port));
                                let t2 = try!(self.pir_retr(bucket, 1, 0, idx2, len1, scope, port));

                                // fake request
                                try!(self.pir_retr(bucket, 2, 0, rng.next_u64() % len0, len0, scope, port));

                                (t1, t2)
                            }

                            // Case 3: label 1 is in collection 1, and label 2 in collection 0
                            (_, Ordering::Less) => {
                                let idx1 = some_or_random!(util::get_idx_bloom(b1, &label1, len1), rng, len1);
                                let idx2 = some_or_random!(util::get_idx_bloom(b0, &label2, len0), rng, len0);

                                let t2 = try!(self.pir_retr(bucket, 0, 0, idx2, len0, scope, port));
                                let t1 = try!(self.pir_retr(bucket, 1, 0, idx1, len1, scope, port));

                                // fake request
                                try!(self.pir_retr(bucket, 2, 0, rng.next_u64() % len0, len0, scope, port));

                                (t1, t2)
                            }

                            // Case 4: both labels fall in collection 1
                            (_, _) => {
                                let idx1 = some_or_random!(util::get_idx_bloom(b1, &label1, len1), rng, len1);
                                let idx2 = some_or_random!(util::get_idx_bloom(b1, &label2, len1), rng, len1);

                                let t1 = try!(self.pir_retr(bucket, 0, 0, idx1, len0, scope, port));
                                let t2 = try!(self.pir_retr(bucket, 1, 0, idx2, len1, scope, port));
                                let t3 = try!(self.pir_retr(bucket, 2, 0, idx1, len0, scope, port));

                                ((&t1 ^ &t3), t2)
                            }
                        };

                        if t1.label() == &label1[..] {
                            // decrypt ciphertext using shared key and insert it into message list
                            let m = try!(pcrypto::decrypt(&peer1.keys.k_e[..], self.round, t1.cipher(), t1.mac()));
                            messages.push(m);
                        }

                        if t2.label() == &label2[..] {
                            // decrypt ciphertext using shared key and insert it into message list
                            let m = try!(pcrypto::decrypt(&peer2.keys.k_e[..], self.round, t2.cipher(), t2.mac()));
                            messages.push(m);
                        }
                    }
                }
            }

            db::RetScheme::Tree => {

                for _ in 0..retries {
                    for bucket in 0..self.partitions.len() {

                        // Get 2 labels to retrieve
                        let (peer1, label1) = self.next_label(&mut bucket_map, bucket, dummy, &mut dummy_count);
                        let (peer2, label2) = self.next_label(&mut bucket_map, bucket, dummy, &mut dummy_count);

                        // Number of tuples and lowest level in collection 1 (lmid)
                        let num = self.buckets[bucket].num_tuples();
                        let lmid = self.buckets[bucket].get_lmid(0);

                        // Compare labels to lmid
                        let cmp1 = util::label_cmp(&label1[..], lmid);
                        let cmp2 = util::label_cmp(&label2[..], lmid);

                        // number of elements in collections 0 and 2
                        let len0 = util::collection_len(num, 0, 2) as u64;
                        // number of elements in collections 1
                        let len1 = util::collection_len(num, 1, 2) as u64;

                        // "_" stands for ">="
                        let (t1, t2) = match (cmp1, cmp2) {

                            // Case 1: both labels fall in collection 0
                            (Ordering::Less, Ordering::Less) => {

                                let t1 = try!(self.bst_retr(&label1[..], bucket, 0, len0, &mut rng, scope, port));

                                let t2 = try!(self.bst_joint_retr(
                                        &label2[..], bucket, 1, len1, len0, &mut rng, scope, port));

                                (t1, t2)
                            }

                            // Case 2: label 1 is in collection 0, and label 2 in collection 1
                            (Ordering::Less, _) => {

                                let t1 = try!(self.bst_retr(&label1[..], bucket, 0, len0, &mut rng, scope, port));

                                let t2 = try!(self.bst_retr(&label2[..], bucket, 1, len1, &mut rng, scope, port));

                                // Generate dummy label
                                let dummy = pcrypto::gen_label(&dummy.keys.k_l[..],
                                                               self.round,
                                                               dummy.uid_self,
                                                               dummy_count,
                                                               0);
                                dummy_count += 1;

                                try!(self.bst_retr(&dummy[..], bucket, 2, len0, &mut rng, scope, port));


                                (t1, t2)
                            }

                            // Case 3: label 1 is in collection 1, and label 2 in collection 0
                            (_, Ordering::Less) => {

                                let t2 = try!(self.bst_retr(&label2[..], bucket, 0, len0, &mut rng, scope, port));

                                let t1 = try!(self.bst_retr(&label1[..], bucket, 1, len1, &mut rng, scope, port));

                                // Generate dummy label
                                let dummy = pcrypto::gen_label(&dummy.keys.k_l[..],
                                                               self.round,
                                                               dummy.uid_self,
                                                               dummy_count,
                                                               0);
                                dummy_count += 1;

                                try!(self.bst_retr(&dummy[..], bucket, 2, len0, &mut rng, scope, port));

                                (t1, t2)
                            }

                            // Case 4: both labels fall in collection 1
                            //
                            // XXX: As written this may leak information since joint retrieval
                            // requests from 0 and 2 and then bst_retr gets from 1.
                            // To fix this one needs to request from 0, 1, 2 (or in parallel).
                            // This leads to slightly more gross code.
                            // Performance-wise this should be no different though.
                            (_, _) => {

                                let t2 = try!(self.bst_joint_retr(
                                        &label2[..], bucket, 0, len0, len0, &mut rng, scope, port));

                                let t1 = try!(self.bst_retr(&label1[..], bucket, 1, len1, &mut rng, scope, port));

                                (t1, t2)
                            }
                        };

                        if let Some(t) = t1 {
                            // decrypt ciphertext 1 using shared key and insert it into message list
                            let m = try!(pcrypto::decrypt(&peer1.keys.k_e[..], self.round, t.cipher(), t.mac()));
                            messages.push(m);
                        }

                        if let Some(t) = t2 {
                            // decrypt ciphertext 2 using shared key and insert it into message list
                            let m = try!(pcrypto::decrypt(&peer2.keys.k_e[..], self.round, t.cipher(), t.mac()));
                            messages.push(m);
                        }
                    }
                }

            }
        }

        Ok(messages)
    }


    fn retr_hybrid4(&'a self,
                    mut bucket_map: HashMap<usize, Vec<(&'a PungPeer, Vec<u8>)>>,
                    scope: &gj::WaitScope,
                    port: &mut gjio::EventPort)
                    -> Result<Vec<Vec<u8>>, Error> {

        let dummy = &self.peers["dummy"];
        let mut dummy_count = 0;
        let mut rng = rand::ChaChaRng::new_unseeded();
        let mut messages: Vec<Vec<u8>> = Vec::new();


        match self.ret_scheme {

            // XXX: The function below probes all collections (as it should) but it does
            // so in an order that is dependent on the labels of interest to the user.
            // This can likely leak information. The solution is to retrieve from the collections
            // in a fixed order (e.g., 0, 1, 2,..., 8) and then put the tuples together afterwards.
            // However, that requires much grosser looking code and its performance is the same
            // as the scheme below. We leave it to be fixed later.
            db::RetScheme::Explicit => {

                // Get labels explicitly
                let explicit_labels = try!(self.get_explicit_labels(scope, port));

                for bucket in 0..self.partitions.len() {

                    // Available collections
                    let mut available: HashSet<usize> = (0..9).collect();

                    // Get 4 (peer, label) to retrieve
                    let mut label_list = Vec::with_capacity(4);
                    label_list.push(self.next_label(&mut bucket_map, bucket, dummy, &mut dummy_count));
                    label_list.push(self.next_label(&mut bucket_map, bucket, dummy, &mut dummy_count));
                    label_list.push(self.next_label(&mut bucket_map, bucket, dummy, &mut dummy_count));
                    label_list.push(self.next_label(&mut bucket_map, bucket, dummy, &mut dummy_count));

                    let lmids = self.buckets[bucket].get_lmids();
                    let bucket_labels = &explicit_labels[&bucket];

                    for &(peer, ref label) in &label_list {

                        let mut c_i = 3; // last collection

                        // Find out in which of the systematic collections does this label fall
                        for (i, lmid) in lmids.iter().enumerate() {
                            if util::label_cmp(&label[..], &lmid[..]) == Ordering::Less {
                                c_i = i;
                                break;
                            }
                        }


                        // Get labels and index of tuple in the target collection (0, 1, 2 or 3)
                        let c_labels = bucket_labels.get(&c_i).unwrap();
                        let idx = some_or_random!(util::get_index(c_labels, &label), rng, c_labels.len() as u64);

                        for parts in &self.h4_mappings[&c_i] {

                            let res = available.is_superset(parts);

                            if res {
                                // All needed parts are available

                                let mut tuple = db::PungTuple::default();

                                for part in parts {

                                    // Remove parts from available set
                                    available.remove(part);


                                    let len = if *part == 4 || *part == 6 || *part == 8 {
                                        bucket_labels.get(&0).unwrap().len() as u64
                                    } else if *part == 5 {
                                        bucket_labels.get(&2).unwrap().len() as u64
                                    } else if *part == 7 {
                                        bucket_labels.get(&1).unwrap().len() as u64
                                    } else {
                                        bucket_labels.get(part).unwrap().len() as u64
                                    };

                                    assert!(idx < len);

                                    // Create the tuple by requesting parts and XORING them together
                                    tuple ^= try!(self.pir_retr(bucket, *part as u32, 0, idx, len, scope, port));
                                }

                                if tuple.label() == &label[..] {
                                    // decrypt ciphertext using shared key and insert it into message list
                                    let m = try!(pcrypto::decrypt(&peer.keys.k_e[..],
                                                                  self.round,
                                                                  tuple.cipher(),
                                                                  tuple.mac()));
                                    messages.push(m);
                                }

                                break;
                            }
                        }
                    }


                    // Once all labels have been retrieved, retrieve from the remaining collections
                    for part in &available {

                        let len = if *part == 4 || *part == 6 || *part == 8 {
                            bucket_labels.get(&0).unwrap().len() as u64
                        } else if *part == 5 {
                            bucket_labels.get(&2).unwrap().len() as u64
                        } else if *part == 7 {
                            bucket_labels.get(&1).unwrap().len() as u64
                        } else {
                            bucket_labels.get(part).unwrap().len() as u64
                        };

                        let idx = rng.next_u64() % len;

                        try!(self.pir_retr(bucket, *part as u32, 0, idx, len, scope, port));
                    }
                }

            }

            // XXX: The function below probes all collections (as it should) but it does
            // so in an order that is dependent on the labels of interest to the user.
            // This can likely leak information. The solution is to retrieve from the collections
            // in a fixed order (e.g., 0, 1, 2,..., 8) and then put the tuples together afterwards.
            // However, that requires much grosser looking code and its performance is the same
            // as the scheme below. We leave it to be fixed later.
            db::RetScheme::Bloom => {

                // Get labels explicitly
                let bloom_filters = try!(self.get_bloom_filter(scope, port));

                for bucket in 0..self.partitions.len() {

                    // Available collections
                    let mut available: HashSet<usize> = (0..9).collect();

                    // Get 4 (peer, label) to retrieve
                    let mut label_list = Vec::with_capacity(4);
                    label_list.push(self.next_label(&mut bucket_map, bucket, dummy, &mut dummy_count));
                    label_list.push(self.next_label(&mut bucket_map, bucket, dummy, &mut dummy_count));
                    label_list.push(self.next_label(&mut bucket_map, bucket, dummy, &mut dummy_count));
                    label_list.push(self.next_label(&mut bucket_map, bucket, dummy, &mut dummy_count));

                    let lmids = self.buckets[bucket].get_lmids();
                    let bucket_blooms = &bloom_filters[&bucket];
                    let num = self.buckets[bucket].num_tuples();

                    for &(peer, ref label) in &label_list {

                        let mut c_i = 3; // last collection

                        // Find out in which of the systematic collections does this label fall
                        for (i, lmid) in lmids.iter().enumerate() {
                            if util::label_cmp(&label[..], &lmid[..]) == Ordering::Less {
                                c_i = i;
                                break;
                            }
                        }


                        // Get labels and index of tuple in the target collection (0, 1, 2 or 3)
                        let c_num = util::collection_len(num, c_i as u32, 4);
                        let c_bloom = bucket_blooms.get(&c_i).unwrap();
                        let idx = some_or_random!(util::get_idx_bloom(c_bloom, &label, c_num), rng, c_num);

                        for parts in &self.h4_mappings[&c_i] {

                            let res = available.is_superset(parts);

                            if res {
                                // All needed parts are available

                                let mut tuple = db::PungTuple::default();

                                for part in parts {

                                    // Remove parts from available set
                                    available.remove(part);


                                    let len = if *part == 4 || *part == 6 || *part == 8 {
                                        util::collection_len(num, 0, 4)
                                    } else if *part == 5 {
                                        util::collection_len(num, 2, 4)
                                    } else if *part == 7 {
                                        util::collection_len(num, 1, 4)
                                    } else {
                                        util::collection_len(num, *part as u32, 4)
                                    };

                                    assert!(idx < len || idx == len);

                                    // The index is not in this part (but it is in the other parts)
                                    // Just fetch anything from this part and ignore the result
                                    if idx == len {
                                        let tmp_idx = rng.next_u64() % (len as u64);
                                        try!(self.pir_retr(bucket, *part as u32, 0, tmp_idx, len, scope, port));
                                    } else {
                                        // Create the tuple by requesting the part and XORING it to prior parts
                                        tuple ^= try!(self.pir_retr(
                                                    bucket, *part as u32, 0, idx, len, scope, port));
                                    }
                                }

                                if tuple.label() == &label[..] {
                                    // decrypt ciphertext using shared key and insert it into message list
                                    let m = try!(pcrypto::decrypt(&peer.keys.k_e[..],
                                                                  self.round,
                                                                  tuple.cipher(),
                                                                  tuple.mac()));
                                    messages.push(m);
                                }

                                break;
                            }
                        }
                    }


                    // Once all labels have been retrieved, retrieve from the remaining collections
                    for part in &available {

                        let len = if *part == 4 || *part == 6 || *part == 8 {
                            util::collection_len(num, 0, 4)
                        } else if *part == 5 {
                            util::collection_len(num, 2, 4)
                        } else if *part == 7 {
                            util::collection_len(num, 1, 4)
                        } else {
                            util::collection_len(num, *part as u32, 4)
                        };

                        let idx = rng.next_u64() % len;

                        try!(self.pir_retr(bucket, *part as u32, 0, idx, len, scope, port));
                    }
                }

            }

            // TODO, FIXME: Previous implementation was horribly inefficient and leaked information.
            // A re-write is work in progress.
            db::RetScheme::Tree => unimplemented!(),

        }

        Ok(messages)
    }



    // Retrieves a tuple from the server given a bucket, collection, level, and index
    fn pir_retr(&self,
                bucket: usize,
                collection: u32,
                level: u32,
                idx: u64,
                len: u64,
                scope: &gj::WaitScope,
                port: &mut gjio::EventPort)
                -> Result<db::PungTuple, Error> {

        // set up PIR handler
        // compute ideal alpha
        let alpha = util::get_alpha(len);
        self.pir_handler.update_params(db::TUPLE_SIZE as u64, len, alpha);

        // Create PIR request
        let query = self.pir_handler.gen_query(idx);
        let mut request = self.conn.retr_request();
        request.get().set_id(self.id);
        request.get().set_round(self.round);
        request.get().set_bucket(bucket as u32);
        request.get().set_collection(collection);
        request.get().set_level(level);
        request.get().set_query(query.query);
        request.get().set_qnum(query.num);

        println!("Upload (pir) {} bytes", 32 + query.query.len());

        // Send request to the server and get response
        let response = try!(request.send().promise.wait(scope, port));

        // Extract PIR answer from response
        let answer: &[u8] = try!(try!(response.get()).get_answer());
        let a_num: u64 = try!(response.get()).get_anum();

        if answer.len() == 0 || a_num == 0 {
            return Err(Error::failed("Invalid PIR answer returned.".to_string()));
        }

        // Decode answer to get tuple
        let decoded = self.pir_handler.decode_answer(answer, a_num);

        println!("Download (pir) {} bytes", 8 + answer.len());

        Ok(db::PungTuple::new(decoded.result))
    }

    // Retrieves a tuple using only a label by searching on the server
    fn bst_retr(&self,
                label: &[u8],
                bucket: usize,
                collection: u32,
                num: u64,
                rng: &mut rand::ChaChaRng,
                scope: &gj::WaitScope,
                port: &mut gjio::EventPort)
                -> Result<Option<db::PungTuple>, Error> {

        let tree_height = util::tree_height(num);
        let mut idx = 0; // first index is 0
        let mut len = 1; // first level has 1 entry
        let mut result: Option<db::PungTuple> = None;

        // Request level by level
        for h in 0..tree_height {

            let tuple = try!(self.pir_retr(bucket, collection, h, idx, len, scope, port));

            if result.is_none() {
                if tuple.gt(label) {
                    // if L* < L
                    idx *= 2;
                } else if tuple.lt(label) {
                    // if L* > L
                    idx = (2 * idx) + 1;
                } else {
                    result = Some(tuple);
                }
            }

            len = 2u64.pow(h + 1);

            // if next round is the last
            if h + 1 == tree_height - 1 {
                len = num - (len - 1);
            }

            if idx >= len || result.is_some() {
                idx = rng.next_u64() % len;
            }
        }

        Ok(result)
    }


    fn bst_joint_retr(&self,
                      label: &[u8],
                      bucket: usize,
                      collection: u32,
                      num: u64,
                      num2: u64, // num of items in collection 2
                      rng: &mut rand::ChaChaRng,
                      scope: &gj::WaitScope,
                      port: &mut gjio::EventPort)
                      -> Result<Option<db::PungTuple>, Error> {

        assert!(num2 == num || num2 == num + 1);

        let tree_height = util::tree_height(num);
        let tree_height2 = util::tree_height(num2);

        let mut idx = 0; // first index is 0
        let mut len = 1; // first level has 1 entry
        let mut result: Option<db::PungTuple> = None;

        // Request level by level
        for h in 0..tree_height - 1 {

            let t1 = try!(self.pir_retr(bucket, collection, h, idx, len, scope, port));
            let t2 = try!(self.pir_retr(bucket, 2, h, idx, len, scope, port));

            let tuple = &t1 ^ &t2;

            if result.is_none() {
                if tuple.gt(label) {
                    // if L* < L
                    idx *= 2;
                } else if tuple.lt(label) {
                    // if L* > L
                    idx = (2 * idx) + 1;
                } else {
                    result = Some(tuple);
                }
            }

            len = 2u64.pow(h + 1);

            if result.is_some() {
                idx = rng.next_u64() % len;
            }
        }

        // Last level has 3 special cases.

        let h = tree_height - 1;
        len = num - (len - 1);

        // Case 1 and 2: both collections have the same number of elements on all shared levels
        if num == num2 || tree_height < tree_height2 {

            if idx >= len || result.is_some() {
                idx = rng.next_u64() % len;
            }

            let t1 = try!(self.pir_retr(bucket, collection, h, idx, len, scope, port));
            let t2 = try!(self.pir_retr(bucket, 2, h, idx, len, scope, port));

            if result.is_none() {
                let tuple = &t1 ^ &t2;

                if tuple.label() == label {
                    result = Some(tuple);
                }
            }


            // Case 2: collection 2 has 1 extra node which causes the tree to have 1 more level
            if tree_height < tree_height2 {
                len = num2 - (len - 1);
                assert_eq!(len, 1);

                // This is pretty wasteful :(. Optimization is to just fetch it normally
                let tuple = try!(self.pir_retr(bucket, 2, h + 1, 0, 1, scope, port));

                if result.is_none() && tuple.label() == label {
                    result = Some(tuple);
                }
            }

            // Case 3: collections differ in 1 element on a shared level
        } else {

            let mut idx2 = idx;
            let len2 = len + 1;

            if idx >= len || result.is_some() {
                idx = rng.next_u64() % len;
            }

            if idx2 >= len2 || result.is_some() {
                idx2 = rng.next_u64() % len2;
            }

            let t1 = try!(self.pir_retr(bucket, collection, h, idx, len, scope, port));
            let t2 = try!(self.pir_retr(bucket, 2, h, idx2, len2, scope, port));

            if result.is_none() {

                // If the same node was not fetched, then just use t2. Otherwise use combination
                let tuple = if idx != idx2 { t2 } else { &t1 ^ &t2 };

                if tuple.label() == label {
                    result = Some(tuple);
                }
            }

        }

        Ok(result)
    }

    pub fn retr(&self,
                peer_names: &[&str],
                scope: &gj::WaitScope,
                port: &mut gjio::EventPort)
                -> Result<Vec<Vec<u8>>, Error> {


        if peer_names.len() as u32 > self.ret_rate {
            return Err(Error::failed("Number of peers exceeds rate".to_string()));
        }

        let bucket_map = try!(self.schedule(peer_names));

        match self.opt_scheme {
            db::OptScheme::Normal | db::OptScheme::Aliasing => self.retr_normal(bucket_map, scope, port),
            db::OptScheme::Hybrid2 => self.retr_hybrid2(bucket_map, scope, port),
            db::OptScheme::Hybrid4 => self.retr_hybrid4(bucket_map, scope, port),
        }
    }
}
