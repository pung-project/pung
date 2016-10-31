//! This module contains the collection of Pung's messages.

use std::cell::RefCell;
use std::rc::Rc;
use std::slice;
use util;

/// Size of a label in Pung (256 bits due to HMAC-SHA256 PRF).
pub const LABEL_SIZE: usize = 32;

/// Size of ciphertext in Pung (256 bytes, due to 256-byte message limit). See
/// [client] (../client/pcrypto/index.html).
pub const CIPHER_SIZE: usize = 238;

/// Size of the message authentication code (128-bits, due to
/// [Poly1305 MAC](../../crypto/poly1305/index.html)).
pub const MAC_SIZE: usize = 16;

/// Size of a Pung tuple (sum of label, cipher, and mac).
pub const TUPLE_SIZE: usize = LABEL_SIZE + CIPHER_SIZE + MAC_SIZE;

/// False positive probability for bloom filter
pub const BLOOM_FP: f64 = 0.00001;

/// Type of retrieval scheme. Explicit retrieval has a single level, tree retrieval
/// constructs a complete binary search tree.
#[derive(PartialEq, Eq, Copy, Clone)]
pub enum RetScheme {
    Explicit,
    Bloom,
    Tree,
}


/// Type of optimization for retrieval scheme.
#[derive(PartialEq, Eq, PartialOrd, Copy, Clone)]
pub enum OptScheme {
    Normal, // No optimization
    Aliasing, // Storing messages under two labels
    Hybrid2, // Hybrid with batch codes (supports 2 collisions per bucket) 
    Hybrid4, // Hybrid with batch codes (supports 4 collisions per bucket) 
}


/// A tuple made up of a label that identifies the message in the Pung cluster, and
/// an encrypted message.
pub struct PungTuple {
    pub data: [u8; TUPLE_SIZE],
}

mod tuple;
pub mod bst;

use db::bst::BSTOrder;
use pir::pir_server::PirServer;

pub type DatabasePtr = Rc<RefCell<Database<'static>>>;

pub struct Database<'a> {
    buckets: Vec<Bucket<'a>>,
}

pub struct Bucket<'a> {
    collections: Vec<Collection<'a>>,
    opt_scheme: OptScheme,
    ret_scheme: RetScheme,
}

/// A collection made up of [`PungTuples`] (struct.`PungTuple`.html).
/// Each [timely dataflow](../../timely/index.html) worker has a copy of the entire
/// collection, which they construct during the send phase of Pung.
/// This is preferable to sharding the database since we obtain parallelism via
/// request sharding rather than data sharding.
pub struct Collection<'a> {
    set: Vec<PungTuple>,
    ret_scheme: RetScheme,
    pir_dbs: Vec<PirServer<'a>>,
    depth: u64,
    bloom: util::bloomfilter::Bloom,
}

impl<'a> Database<'a> {
    pub fn new(ret_scheme: RetScheme, opt_scheme: OptScheme, buckets: usize, depth: u64) -> Database<'a> {

        let mut db = Database { buckets: Vec::new() };

        for _ in 0..buckets {
            let bucket = Bucket::new(ret_scheme, opt_scheme, depth);
            db.buckets.push(bucket);
        }

        db
    }

    /// Total number of subcollections in the database
    #[inline]
    pub fn total_dbs(&self) -> usize {
        let mut count = 0;

        for bucket in &self.buckets {
            count += bucket.total_dbs();
        }

        count
    }

    /// Total number of tuples in the database
    #[inline]
    pub fn len(&self) -> usize {
        let mut count = 0;

        for bucket in &self.buckets {
            count += bucket.len();
        }

        count
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    #[inline]
    pub fn opt_scheme(&self) -> OptScheme {
        self.buckets[0].opt_scheme()
    }

    /// Total number of buckets in the database
    #[inline]
    pub fn num_buckets(&self) -> usize {
        self.buckets.len()
    }

    #[inline]
    pub fn get_buckets(&self) -> slice::Iter<Bucket> {
        self.buckets.iter()
    }

    #[inline]
    pub fn get_bucket(&self, id: usize) -> &'a Bucket {
        &self.buckets[id]
    }

    #[inline]
    pub fn get_bucket_mut(&mut self, id: usize) -> &'a mut Bucket {
        &mut self.buckets[id]
    }

    #[inline]
    pub fn clear(&mut self) {
        for bucket in &mut self.buckets {
            bucket.clear();
        }
    }

    #[inline]
    pub fn push(&mut self, bucket_id: usize, tuple: PungTuple) {
        self.buckets[bucket_id].push(tuple);
    }

    #[inline]
    pub fn encode(&mut self) {
        for bucket in &mut self.buckets {
            bucket.encode();
        }
    }

    #[inline]
    pub fn pir_setup(&mut self) {
        for bucket in &mut self.buckets {
            bucket.pir_setup();
        }
    }
}

impl<'a> Bucket<'a> {
    pub fn new(ret_scheme: RetScheme, opt_scheme: OptScheme, depth: u64) -> Bucket<'a> {
        let mut b = Bucket { collections: Vec::new(), opt_scheme: opt_scheme, ret_scheme: ret_scheme };

        // Default is 1 collection
        b.collections.push(Collection::new(ret_scheme, depth));

        // Hybrid 2 adds 2 more collections, Hybrid 4 adds 8 more
        if opt_scheme == OptScheme::Hybrid2 {
            b.collections.push(Collection::new(ret_scheme, depth));
            b.collections.push(Collection::new(ret_scheme, depth));
        } else if opt_scheme == OptScheme::Hybrid4 {
            for _ in 0..8 {
                b.collections.push(Collection::new(ret_scheme, depth));
            }
        }

        b
    }

    #[inline]
    pub fn get_collections(&self) -> slice::Iter<Collection> {
        self.collections.iter()
    }

    #[inline]
    pub fn get_collection(&self, id: usize) -> &'a Collection {
        &self.collections[id]
    }

    #[inline]
    pub fn get_collection_mut(&mut self, id: usize) -> &'a mut Collection {
        &mut self.collections[id]
    }

    #[inline]
    pub fn num_collections(&self) -> usize {
        self.collections.len()
    }

    #[inline]
    pub fn total_dbs(&self) -> usize {
        let mut count = 0;
        for collection in &self.collections {
            count += collection.num_levels();
        }

        count
    }

    #[inline]
    pub fn len(&self) -> usize {
        let mut count = 0;
        for collection in &self.collections {
            count += collection.len();
        }

        count
    }


    #[inline]
    pub fn unencoded_len(&self) -> usize {

        let mut count = self.collections[0].len();

        if self.opt_scheme == OptScheme::Hybrid2 {
            count += self.collections[1].len();
        } else if self.opt_scheme == OptScheme::Hybrid4 {
            count += self.collections[1].len();
            count += self.collections[2].len();
            count += self.collections[3].len();
        }

        count
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    #[inline]
    pub fn clear(&mut self) {
        for collection in &mut self.collections {
            collection.clear();
        }
    }

    #[inline]
    pub fn opt_scheme(&self) -> OptScheme {
        self.opt_scheme
    }

    // Pushes always go to the 0'th colletion. Encoding takes care of spreading them around
    #[inline]
    pub fn push(&mut self, tuple: PungTuple) {
        self.collections[0].push(tuple);
    }

    #[inline]
    pub fn encode(&mut self) {

        // Sort collection
        self.collections[0].sort();

        if (self.opt_scheme == OptScheme::Normal || self.opt_scheme == OptScheme::Aliasing) &&
           self.ret_scheme == RetScheme::Tree {

            self.collections[0].as_bst_array();

        } else if (self.opt_scheme == OptScheme::Normal || self.opt_scheme == OptScheme::Aliasing) &&
                  self.ret_scheme == RetScheme::Bloom {

            self.collections[0].set_bloom();

        } else if self.opt_scheme == OptScheme::Hybrid2 {

            assert_eq!(self.collections.len(), 3);

            let len = self.len();

            // Get the first half which has all tuples and split it in half
            let tuples = self.collections[0].split_off((len + 1) / 2);

            // Setup the second collection with the remaining items
            self.collections[1].set_contents(tuples);

            assert!(self.collections[0].len() == self.collections[1].len() ||
                    self.collections[0].len() == self.collections[1].len() + 1);

            // If we are doing BST retrieval or Bloom
            if self.ret_scheme == RetScheme::Tree {
                self.collections[0].as_bst_array();
                self.collections[1].as_bst_array();
            } else if self.ret_scheme == RetScheme::Bloom {
                self.collections[0].set_bloom();
                self.collections[1].set_bloom();
            }


            // XOR tuples with each other
            let mut xor_tuples: Vec<PungTuple> = self.collections[0]
                .get_tuples()
                .zip(self.collections[1].get_tuples())
                .map(|(a, b)| a ^ b)
                .collect();

            // Missing one of them due to odd number of tuples. Get it from the first collection.
            if xor_tuples.len() != self.collections[0].len() {
                xor_tuples.push(self.collections[0].get_tuple(self.collections[0].len() - 1).clone());
            }

            self.collections[2].set_contents(xor_tuples);

            assert_eq!(self.collections[0].len(), self.collections[2].len());

        } else if self.opt_scheme == OptScheme::Hybrid4 {

            assert_eq!(self.collections.len(), 9);

            let mut len = self.len();

            // Split collection 0 (which has all the tuples) in half
            let mut collection_2 = self.collections[0].split_off((len + 1) / 2);

            len = self.collections[0].len();

            // Split collection 0 (which has half the tuples) in half again
            let collection_1 = self.collections[0].split_off((len + 1) / 2);

            len = collection_2.len();

            // Split collection 2 (which has half the tuples) in half
            let collection_3 = collection_2.split_off((len + 1) / 2);

            // Now all collections have 1/4 of the tuples
            self.collections[1].set_contents(collection_1);
            self.collections[2].set_contents(collection_2);
            self.collections[3].set_contents(collection_3);

            // If we are doing BST retrieval, convert to BSTs
            if self.ret_scheme == RetScheme::Tree {

                for i in 0..4 {
                    self.collections[i].as_bst_array();
                }

            } else if self.ret_scheme == RetScheme::Bloom {

                for i in 0..4 {
                    self.collections[i].set_bloom();
                }
            }

            // Encode (XOR) collections as follows

            let plan = [(0, 1), (2, 3), (0, 2), (1, 3), (6, 7)];

            for (i, &(c1, c2)) in plan.iter().enumerate() {

                let mut collection_i: Vec<PungTuple> = self.collections[c1]
                    .get_tuples()
                    .zip(self.collections[c2].get_tuples())
                    .map(|(a, b)| a ^ b)
                    .collect();

                // Missing one of them due to odd number of tuples. Get it from the first collection.
                if collection_i.len() != self.collections[c1].len() {
                    collection_i.push(self.collections[c1].get_tuple(self.collections[c1].len() - 1).clone());
                }

                self.collections[i + 4].set_contents(collection_i);
            }


            // Check the right numbers are present
            for i in 0..4 {
                assert_eq!(self.collections[i].len() as u64,
                           util::collection_len(self.unencoded_len() as u64, i as u32, 4));
            }
        }

    }

    #[inline]
    pub fn mid_labels(&self) -> Vec<Vec<u8>> {

        if self.opt_scheme == OptScheme::Hybrid2 {

            let lmid = match self.ret_scheme {
                RetScheme::Explicit | RetScheme::Bloom => {

                    // lmid is the first element
                    match self.collections[1].get_first() {
                        Some(v) => v.label().to_vec(),
                        None => vec![],
                    }
                }

                RetScheme::Tree => {
                    // lmid is the most bottom-left element

                    if !self.collections[1].is_empty() {
                        let h = util::tree_height(self.collections[1].len() as u64);
                        let lmid = self.collections[1].get_tuple((2u64.pow(h - 1) - 1) as usize);
                        lmid.label().to_vec()
                    } else {
                        vec![]
                    }
                }
            };

            vec![lmid]

        } else if self.opt_scheme == OptScheme::Hybrid4 {

            let mut lmids = Vec::with_capacity(3);

            for i in 1..4 {
                let lmid = match self.ret_scheme {
                    RetScheme::Explicit | RetScheme::Bloom => {
                        // lmid is the first element
                        match self.collections[i].get_first() {
                            Some(v) => v.label().to_vec(),
                            None => vec![],
                        }
                    }

                    RetScheme::Tree => {
                        // lmid is th emost bottom-left element
                        if !self.collections[i].is_empty() {
                            let h = util::tree_height(self.collections[i].len() as u64);
                            let lmid = self.collections[i].get_tuple((2u64.pow(h - 1) - 1) as usize);
                            lmid.label().to_vec()
                        } else {
                            vec![]
                        }
                    }
                };

                lmids.push(lmid);
            }

            lmids

        } else {
            vec![]
        }
    }

    #[inline]
    pub fn pir_setup(&mut self) {
        for collection in &mut self.collections {
            if !collection.is_empty() {
                collection.pir_setup();
            }
        }
    }
}


impl<'a> Collection<'a> {
    /// Creates a new empty Collection.
    pub fn new(ret_scheme: RetScheme, depth: u64) -> Collection<'a> {

        Collection {
            set: Vec::new(),
            ret_scheme: ret_scheme,
            pir_dbs: Vec::new(),
            depth: depth,
            bloom: util::bloomfilter::Bloom::new(1, 1),
        }
    }

    /// Returns all labels
    #[inline]
    pub fn get_label(&'a self, idx: usize) -> &'a [u8] {
        self.set[idx].label()
    }

    #[inline]
    pub fn get_bloom(&'a self) -> &'a util::bloomfilter::Bloom {
        &self.bloom
    }


    /// Returns the number of tuples in the bucket.
    #[inline]
    pub fn len(&self) -> usize {
        self.set.len()
    }


    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns the number of levels in the tree representing a bucket's collection
    #[inline]
    pub fn num_levels(&self) -> usize {
        if self.ret_scheme == RetScheme::Tree {
            util::tree_height(self.set.len() as u64) as usize
        } else {
            1
        }
    }

    /// Adds a tuple to the end of the collection.
    #[inline]
    pub fn push(&mut self, tuple: PungTuple) {
        self.set.push(tuple)
    }

    #[inline]
    pub fn get_first(&'a self) -> Option<&'a PungTuple> {
        self.set.first()
    }

    #[inline]
    pub fn get_tuple(&'a self, idx: usize) -> &'a PungTuple {
        &self.set[idx]
    }

    #[inline]
    pub fn get_tuples(&self) -> slice::Iter<PungTuple> {
        self.set.iter()
    }

    #[inline]
    pub fn set_contents(&mut self, collection: Vec<PungTuple>) {
        self.set = collection;
    }


    pub fn set_bloom(&mut self) {

        let mut bloom = util::bloomfilter::Bloom::new_for_fp_rate(self.len(), BLOOM_FP);

        for (i, t) in self.set.iter().enumerate() {
            bloom.set((i, t.label()));
        }

        self.bloom = bloom;
    }

    #[inline]
    pub fn split_off(&mut self, offset: usize) -> Vec<PungTuple> {
        self.set.split_off(offset)
    }


    #[inline]
    pub fn set_scheme(&mut self, scheme: RetScheme) {
        self.ret_scheme = scheme;
    }

    #[inline]
    pub fn sort(&mut self) {
        self.set.sort();
    }

    /// Changes the ordering of tuples in the collection to one that mirrors
    /// an array representation of a complete binary search tree (i.e.,
    /// this encodes a collection as a complete BST).
    pub fn as_bst_array(&mut self) {

        if self.ret_scheme == RetScheme::Tree {
            self.set.as_bst_order();
        }

    }

    pub fn pir_setup(&mut self) {

        let depth = self.depth;

        let levels = self.num_levels();
        let mut pir_dbs = Vec::with_capacity(levels);

        for i in 0..levels {
            let level: &[PungTuple] = self.get_level(i);
            let alpha = util::get_alpha(level.len() as u64);
            pir_dbs.push(PirServer::new(level, alpha, depth));
        }

        self.pir_dbs = pir_dbs;
    }

    #[inline]
    pub fn pir_handler(&self, level: usize) -> &PirServer {
        &self.pir_dbs[level as usize]
    }

    /// Gets all the Tuples at a particular level in the BST representation.
    #[inline]
    pub fn get_level(&'a self, level: usize) -> &'a [PungTuple] {

        if self.ret_scheme == RetScheme::Explicit || self.ret_scheme == RetScheme::Bloom {

            &self.set[..]

        } else {

            let min = (2u64.pow(level as u32) - 1) as usize;
            let mut max = (2u64.pow(level as u32 + 1) - 1) as usize;

            assert!(min < self.set.len());

            if max > self.set.len() {
                max = self.set.len();
            }

            &self.set[min..max]
        }
    }

    /// Performs garbage collection on the collection (heh...)
    // XXX: For our experiments we just clear all messages
    // In practice, it is more useful if this is a sliding window
    #[inline]
    pub fn clear(&mut self) {
        self.set.clear();
        self.pir_dbs.clear();
    }
}
