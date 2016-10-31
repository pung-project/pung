#![feature(test)]
#![allow(non_snake_case)]

extern crate criterion;
extern crate rand;
extern crate test;
extern crate pung;

use criterion::Bencher;
use std::time::Duration;
use pung::pir::pir_client::PirClient;
use pung::pir::pir_server::PirServer;
use rand::ChaChaRng;
use rand::Rng;

macro_rules! bmark_settings {
    () => {{

        // If you want to change settings call .sample_size() or any of the other options
        //
        // Example:
        let mut crit = criterion::Criterion::default();
        crit.sample_size(25)
            .measurement_time(Duration::new(0, 50000000)); // in (sec, ns)

        crit
    }};

}



macro_rules! pir_server {
    ($name:ident, $num:expr, $alpha:expr, $d:expr, $size:expr) => (

        #[test]
        fn $name() {
            fn $name(b: &mut Bencher) {
                let mut rng = ChaChaRng::new_unseeded();
                let mut x = [0u8; $size];
                rng.fill_bytes(&mut x);

                let mut collection = vec![];
                for _ in 0..$num {
                   collection.push(x);
                }

                b.iter(|| PirServer::new(&mut collection, $alpha, $d));
            }

            let mut bmark = bmark_settings!();
            bmark.bench_function(stringify!($name), $name);
        }
    )
}

macro_rules! pir_client {
    ($name:ident, $num:expr, $alpha:expr, $d:expr, $size:expr) => (

        #[test]
        fn $name() {
            fn $name(b: &mut Bencher) {
                b.iter(|| PirClient::new($size, $num, $alpha, $d));
            }

            let mut bmark = bmark_settings!();
            bmark.bench_function(stringify!($name), $name);

        }
    )
}



macro_rules! pir_query {
    ($name:ident, $num:expr, $alpha:expr, $d:expr, $size:expr) => (

        #[test]
        fn $name() {

            {
                // Measure network / memory
                println!("----------------PUNG QUERY MEMORY RESULT------------------\n");

                let client = PirClient::new($size, $num, $alpha, $d);
                let query = client.gen_query(rand::random::<u64>() % $num);

                println!("{} query size: {} bytes", stringify!($name), query.query.len());

                println!("-----------------------------------------------------\n");
            }


            // Measure time
            fn $name(b: &mut Bencher) {
                let client = PirClient::new($size, $num, $alpha, $d);
                b.iter_with_setup(|| rand::random::<u64>() % $num,
                                  |idx| {
                                     client.gen_query(idx);
                                  });
            }

            let mut bmark = bmark_settings!();
            bmark.bench_function(stringify!($name), $name);
        }
    )
}

macro_rules! pir_answer {
    ($name:ident, $num:expr, $alpha:expr, $d:expr, $size:expr) => (

        #[test]
        fn $name() {

            {
                // Measure network / memory
                println!("----------------PUNG ANSWER MEMORY RESULT------------------\n");
                
                let mut rng = ChaChaRng::new_unseeded();
                let mut x = [0u8; $size];
                rng.fill_bytes(&mut x);

                let mut collection = vec![];

                for _ in 0..$num {
                   collection.push(x);
                }

                let server = PirServer::new(&mut collection, $alpha, $d);
                let client = PirClient::new($size, $num, $alpha, $d);

                let query = client.gen_query(rand::random::<u64>() % $num);
                println!("{} query size: {} bytes", stringify!($name), query.query.len());

                let answer = server.gen_answer(query.query, query.num);
                println!("{} answer size: {} bytes", stringify!($name), answer.answer.len());

                println!("-----------------------------------------------------\n");
            }


            // Measure time

            fn $name(b: &mut Bencher) {
                let mut rng = ChaChaRng::new_unseeded();
                let mut x = [0u8; $size];
                rng.fill_bytes(&mut x);

                let mut collection = vec![];

                for _ in 0..$num {
                   collection.push(x);
                }

                let server = PirServer::new(&mut collection, $alpha, $d);
                let client = PirClient::new($size, $num, $alpha, $d);

                b.iter_with_setup(|| {
                        client.gen_query(rand::random::<u64>() % $num)
                    }, |query| {
                        server.gen_answer(query.query, query.num);
                    });
            }

            let mut bmark = bmark_settings!();
            bmark.bench_function(stringify!($name), $name);
        }
    )
}

macro_rules! pir_decode {
    ($name:ident, $num:expr, $alpha:expr, $d:expr, $size:expr) => (

        #[test]
        fn $name() {

            {
                // Measure network / memory
                println!("----------------PUNG DECODE MEMORY RESULT------------------\n");
                
                let mut rng = ChaChaRng::new_unseeded();
                let mut x = [0u8; $size];
                rng.fill_bytes(&mut x);

                let mut collection = vec![];

                for _ in 0..$num {
                   collection.push(x);
                }

                let server = PirServer::new(&mut collection, $alpha, $d);
                let client = PirClient::new($size, $num, $alpha, $d);

                let query = client.gen_query(rand::random::<u64>() % $num);
                println!("{} query size: {} bytes", stringify!($name), query.query.len());

                let answer = server.gen_answer(query.query, query.num);
                println!("{} answer size: {} bytes", stringify!($name), answer.answer.len());

                let result = client.decode_answer(answer.answer, answer.num);
                println!("{} decoded result size: {} bytes", stringify!($name), result.result.len());

                println!("-----------------------------------------------------\n");
            }


            // Measure time

            fn $name(b: &mut Bencher) {

                let mut rng = ChaChaRng::new_unseeded();
                let mut x = [0u8; $size];
                rng.fill_bytes(&mut x);

                let mut collection = vec![];

                for _ in 0..$num {
                   collection.push(x);
                }


                let server = PirServer::new(&mut collection, $alpha, $d);
                let client = PirClient::new($size, $num, $alpha, $d);

                b.iter_with_setup(|| {
                        let query = client.gen_query(rand::random::<u64>() % $num);
                        server.gen_answer(query.query, query.num)
                    }, |answer| {
                        client.decode_answer(answer.answer, answer.num);
                    });
           }

            let mut bmark = bmark_settings!();
            bmark.bench_function(stringify!($name), $name);
        }
    )
}


// Parameters: 
// bench name, number of entries, alpha, d, size of each entry
pir_query!(bench_pir_query_2048_d_2_a_8_1KB, 2048, 8, 2, 1024);
pir_answer!(bench_pir_answer_2048_d_2_a_8_1KB, 2048, 8, 2, 1024);
pir_decode!(bench_pir_decode_2048_d_2_a_8_1KB, 2048, 8, 2, 1024); 

pir_query!(bench_pir_query_8192_d_2_a_8_1KB, 8192, 8, 2, 1024);
pir_answer!(bench_pir_answer_8192_d_2_a_8_1KB, 8192, 8, 2, 1024);
pir_decode!(bench_pir_decode_8192_d_2_a_8_1KB, 8192, 8, 2, 1024); 

pir_query!(bench_pir_query_32768_d_2_a_16_1KB, 32768, 16, 2, 1024);
pir_answer!(bench_pir_answer_32768_d_2_a_16_1KB, 32768, 16, 2, 1024);
pir_decode!(bench_pir_decode_32768_d_2_a_16_1KB, 32768, 16, 2, 1024); 

pir_query!(bench_pir_query_131072_d_2_a_32_1KB, 131072, 32, 2, 1024);
pir_answer!(bench_pir_answer_131072_d_2_a_32_1KB, 131072, 32, 2, 1024);
pir_decode!(bench_pir_decode_131072_d_2_a_32_1KB, 131072, 32, 2, 1024); 


pir_query!(bench_pir_query_2048_d_2_a_32, 2048, 32, 2, 288);
pir_answer!(bench_pir_answer_2048_d_2_a_32, 2048, 32, 2, 288);
pir_decode!(bench_pir_decode_2048_d_2_a_32, 2048, 32, 2, 288); 

pir_query!(bench_pir_query_8192_d_2_a_32, 8192, 32, 2, 288);
pir_answer!(bench_pir_answer_8192_d_2_a_32, 8192, 32, 2, 288);
pir_decode!(bench_pir_decode_8192_d_2_a_32, 8192, 32, 2, 288); 

pir_query!(bench_pir_query_32768_d_2_a_32, 32768, 32, 2, 288);
pir_answer!(bench_pir_answer_32768_d_2_a_32, 32768, 32, 2, 288);
pir_decode!(bench_pir_decode_32768_d_2_a_32, 32768, 32, 2, 288); 

pir_query!(bench_pir_query_131072_d_2_a_64, 131072, 64, 2, 288);
pir_answer!(bench_pir_answer_131072_d_2_a_64, 131072, 64, 2, 288);
pir_decode!(bench_pir_decode_131072_d_2_a_64, 131072, 64, 2, 288); 

// Test client and server creation

pir_server!(bench_pir_server_creation_2048_1KB, 2048, 8, 2, 1024);
pir_server!(bench_pir_server_creation_8192_1KB, 8192, 8, 2, 1024);
pir_server!(bench_pir_server_creation_32768_1KB, 32768, 16, 2, 1024);
pir_server!(bench_pir_server_creation_131072_1KB, 131072, 32, 2, 1024);

pir_server!(bench_pir_server_creation_2048, 2048, 32, 2, 288);
pir_server!(bench_pir_server_creation_8192, 8192, 32, 2, 288);
pir_server!(bench_pir_server_creation_32768, 32768, 32, 2, 288);
pir_server!(bench_pir_server_creation_131072, 131072, 64, 2, 288);

//pir_client!(bench_pir_client_creation_100, 100, 1, 1, 1);
//pir_client!(bench_pir_client_creation_1k, 1000, 1, 1, 1);
