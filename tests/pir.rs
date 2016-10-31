extern crate rand;
extern crate pung;

use std::mem;
use pung::pir::pir_client::PirClient;
use pung::pir::pir_server::PirServer;
use pung::db::PungTuple;
use rand::Rng;


macro_rules! get_size {
    ($d_type:ty) => (mem::size_of::<$d_type>() as u64);
}

#[test]
fn pir_decode() {
    let num = 6;
    let alpha = 1;
    let d = 1;
    let mut collection: Vec<PungTuple> = Vec::new();

    let mut rng = rand::thread_rng();

    for _ in 0..num {
        let mut x: [u8; 286] = [0; 286];
        rng.fill_bytes(&mut x);

        let pt = PungTuple::new(&x);

        collection.push(pt);
    }

    let truth = collection.clone();

    // Create the client
    let client = PirClient::new(1, 1, alpha, d);

    let first = 0;
    let last = 1;
    let test_num = last - first;

    let server = PirServer::new(&mut collection[first..last], alpha, d);
    client.update_params(get_size!(PungTuple), test_num as u64, alpha);

//    for i in 0..test_num {
    {
        let query = client.gen_query(0 as u64);
        let answer = server.gen_answer(query.query, query.num);
        let result = client.decode_answer(answer.answer, answer.num);
        assert!(PungTuple::new(result.result) == truth[first + 0 as usize]);
    }

    let first = 1;
    let last = 3;
    let test_num = last - first;

    let server_2 = PirServer::new(&mut collection[first..last], alpha, d);
    client.update_params(get_size!(PungTuple), test_num as u64, alpha);

//    for i in 0..test_num {
    {
        let query = client.gen_query(1 as u64);
        let answer = server_2.gen_answer(query.query, query.num);
        let result = client.decode_answer(answer.answer, answer.num);
        assert!(PungTuple::new(result.result) == truth[first + 1 as usize]);
    }

    let first = 3;
    let last = 6;
    let test_num = last - first;


    let server_3 = PirServer::new(&mut collection[first..last], alpha, d);
    client.update_params(get_size!(PungTuple), test_num as u64, alpha);

//    for i in 0..test_num {
    {
        let query = client.gen_query(2 as u64);
        let answer = server_3.gen_answer(query.query, query.num);
        let result = client.decode_answer(answer.answer, answer.num);
        assert!(PungTuple::new(result.result) == truth[first + 2 as usize]);
    }



}
