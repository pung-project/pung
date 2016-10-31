#![feature(test)]
#![allow(non_snake_case)]

extern crate criterion;
extern crate pung;
extern crate quicksort;
extern crate rand;
extern crate test;

use std::time::Duration;
use criterion::Bencher;
use pung::client::pcrypto::*;
use pung::util::bloomfilter;
use pung::db;
use rand::ChaChaRng;
use rand::Rng;

macro_rules! bmark_settings {
    () => {{

        // If you want to change settings call .sample_size() or any of the other options
        //
        // Example:
        let mut crit = criterion::Criterion::default();
        crit.sample_size(20)
            .measurement_time(Duration::new(0, 5000)); // in (sec, ns)
        crit
    }};

}

#[test]
fn bench_derive_keys() {
    fn bench_derive_keys(b: &mut Bencher) {
        let mut rng =ChaChaRng::new_unseeded();
        let mut secret = [0u8; 32];
        rng.fill_bytes(&mut secret);
        
        b.iter(move || {
            test::black_box(derive_keys(&secret));
        });
    }

    let mut bmark = bmark_settings!();
    bmark.bench_function("bench_derive_keys", bench_derive_keys);
}


#[test]
fn bench_gen_label() {

    fn bench_gen_label(b: &mut Bencher) {

        let mut rng =ChaChaRng::new_unseeded();
        let mut secret = [0u8; 32];
        rng.fill_bytes(&mut secret);

        let keys = derive_keys(&secret);
        let round = 0;
        let uid = 0;
        let msg_num = 0;
        let collision_num = 0;

        b.iter(move || {
            test::black_box(gen_label(&keys.k_l[..], round, uid, msg_num, collision_num));
        });
    }

    let mut bmark = bmark_settings!();
    bmark.bench_function("bench_gen_label", bench_gen_label);
}

#[test]
fn bench_encrypt() {
    fn bench_encrypt(b: &mut Bencher) {

        let mut rng =ChaChaRng::new_unseeded();
        let mut secret = [0u8; 32];
        rng.fill_bytes(&mut secret);

        let keys = derive_keys(&secret);
        let round = 0;

        let mut message = [0u8; MESSAGE_SIZE];
        rng.fill_bytes(&mut message);

        b.iter(move || {
            test::black_box(encrypt(&keys.k_e[..], round, &message));
        });
    }

    let mut bmark = bmark_settings!();
    bmark.bench_function("bench_encrypt", bench_encrypt);
}

#[test]
fn bench_decrypt() {
    fn bench_decrypt(b: &mut Bencher) {

        let mut rng =ChaChaRng::new_unseeded();
        let mut secret = [0u8; 32];
        rng.fill_bytes(&mut secret);

        let keys = derive_keys(&secret);
        let round = 0;

        let mut message = [0u8; MESSAGE_SIZE];
        rng.fill_bytes(&mut message);

        let c = encrypt(&keys.k_e[..], round, &message);

        b.iter(move || {
            test::black_box(decrypt(&keys.k_e[..], round, &c.0[..], &c.1[..]).unwrap());
        });
    }

    let mut bmark = bmark_settings!();
    bmark.bench_function("bench_decrypt", bench_decrypt);
}

macro_rules! bloom_filter {
    ($name: ident, $num:expr) => (
        #[test]
        fn $name() {

            // Measure network / memory
            {
                println!("----------------PUNG BLOOMFILTER RESULT--------------\n");
                let bloom_test = bloomfilter::Bloom::new_for_fp_rate($num, db::BLOOM_FP); 
                println!("{} bloomfilter size: {} bytes", stringify!($name), bloom_test.number_of_bits()/8);
                println!("-----------------------------------------------------\n");
            }


            fn $name(b: &mut Bencher) {
                let mut rng = ChaChaRng::new_unseeded();

                b.iter_with_setup(|| {

                    let mut bloom = bloomfilter::Bloom::new_for_fp_rate($num, db::BLOOM_FP);
                    let mut label = [0u8; db::LABEL_SIZE];
                    rng.fill_bytes(&mut label);

                    let chosen = (rng.next_u64() % ($num as u64)) as usize; // chosen index

                    for i in 0..($num as usize) {
                        if i == chosen {
                            bloom.set((i, 1, label));
                        } else {
                            bloom.set((i, 0, label));
                        }
                    }


                    (bloom, label)

                }, |(bloom, label)| { 

                    // Checks all possible ones until it finds the one we want (index, 1, label).
                    for i in 0..($num as usize) {
                        if bloom.check((i, 1, label)) {
                            break;
                        }
                    }
                });
            }

            let mut bmark = bmark_settings!();
            bmark.bench_function(stringify!($name), $name);
        }
    )
}

bloom_filter!(bench_bloom_filter_2048, 2048);
bloom_filter!(bench_bloom_filter_8192, 8192);
bloom_filter!(bench_bloom_filter_32768, 32768);
bloom_filter!(bench_bloom_filter_131072, 131072);
