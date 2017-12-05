use byteorder::{BigEndian, WriteBytesExt};
use db;
use std::cmp;
use std::io::Cursor;

pub mod bloomfilter;

#[macro_export]
macro_rules! retry_bound {
    ($k:expr) => {
        if $k < 9 { // special case since formula yields a very loose upper bound for k < 9
            $k as u32 // we can always retrieve k elements in k rounds
        } else {
            3 * (($k as f64).ln() / ($k as f64).ln().ln()).ceil() as u32
        }
    };

    ($k:expr, $d:expr) => {
        if $k < 3 { // special case since formula yields a very loose upper bound for k < 3
            $k as u32
        } else {
            ((($k as f64).ln().ln() / ($d as f64).ln()) + 1.0).ceil() as u32
        }
    };
}


#[macro_export]
macro_rules! some_or_random {
    ($res:expr, $rng:expr, $len:expr) => {
        if let Some(idx) = $res {
            idx
        } else {
           $rng.next_u64() % ($len as u64)
        }
    };
}

// Below is unsafe
#[inline]
pub fn label_cmp(l1: &[u8], l2: &[u8]) -> cmp::Ordering {
    unsafe {
        (&*(l1 as *const [u8] as *const [u64; 4])).cmp(&*(l2 as *const [u8] as *const [u64; 4]))
    }
}


#[inline]
pub fn tree_height(num: u64) -> u32 {
    ((num + 1) as f64).log2().ceil() as u32
}

#[inline]
pub fn get_index(labels: &[Vec<u8>], label: &[u8]) -> Option<u64> {
    match labels.binary_search_by(|probe| label_cmp(&probe[..], label)) {
        Ok(i) => Some(i as u64),
        Err(_) => None,
    }
}


#[inline]
pub fn get_idx_bloom(bloom: &bloomfilter::Bloom, label: &[u8], num: u64) -> Option<u64> {
    for i in 0..(num as usize) {
        if bloom.check((i, label)) {
            return Some(i as u64);
        }
    }

    None
}


// Returns number of elements in collection for given collection_idx (this assumes hybrid 2 or 4)
pub fn collection_len(bucket_len: u64, collection_idx: u32, num_collections: u32) -> u64 {
    if num_collections == 1 {
        bucket_len
    } else if num_collections == 2 {
        // hybrid 2
        match collection_idx {
            0 => (bucket_len as f64 / 2f64).ceil() as u64,
            1 => bucket_len / 2,
            _ => panic!("Invalid collection idx"),
        }
    } else if num_collections == 4 {
        // hybrid 4
        match collection_idx {
            0 => ((bucket_len as f64 / 2f64).ceil() / 2f64).ceil() as u64,
            1 => ((bucket_len as f64 / 2f64).ceil() / 2f64).floor() as u64,
            2 => ((bucket_len as f64 / 2f64).floor() / 2f64).ceil() as u64,
            3 => bucket_len / 4,
            _ => panic!("Invalid collection idx"),
        }
    } else {
        panic!("Invalid num collections");
    }
}


// Returns the indices of collections that contain a meaningful label
#[inline]
pub fn label_collections(scheme: db::OptScheme) -> Vec<usize> {
    match scheme {
        db::OptScheme::Normal | db::OptScheme::Aliasing => vec![0],
        db::OptScheme::Hybrid2 => vec![0, 1], // labels are in collections 0 and 1
        db::OptScheme::Hybrid4 => vec![0, 1, 2, 3], // labels are in collections 0, 1, 2, and 3
    }
}

#[inline]
pub fn label_marker(index: usize, buckets: usize) -> Vec<u8> {
    assert!(index < buckets);

    let max = u32::max_value();
    let mut limit = max / buckets as u32;
    limit *= (index as u32) + 1;

    let mut a = Cursor::new(Vec::with_capacity(4));
    a.write_u32::<BigEndian>(limit).unwrap();
    a.into_inner()
}

#[inline]
pub fn bucket_idx(label: &[u8], partitions: &[Vec<u8>]) -> usize {
    for (i, partition) in partitions.iter().enumerate() {
        if label <= &partition[..] {
            return i;
        }
    }

    0
}

#[inline]
pub fn get_alpha(num: u64) -> u64 {
    if db::CIPHER_SIZE <= 240 {
        if num < 8 {
            1
        } else if num < 2048 {
            8
        } else if num < 65536 {
            32
        } else {
            64
        }
    } else if db::CIPHER_SIZE <= 1024 {
        if num < 8 {
            1
        } else if num < 32768 {
            8
        } else if num < 131072 {
            16
        } else {
            32
        }
    } else if num < 32768 {
        1
    } else {
        8
    }
}
