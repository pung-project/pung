extern crate criterion;
extern crate pung;
extern crate quicksort;
extern crate rand;

use std::time::Duration;
use std::rc::Rc;
use criterion::Bencher;
use pung::db;
use pung::db::bst::BSTOrder;

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

fn create_db(len: usize, workers: usize, set: &mut Vec<Rc<db::PungTuple>>){

    for _ in 0..workers {

        let mut worker_db = Vec::with_capacity(len);

        for _ in 0..len {
            let mut raw_tuple = Vec::with_capacity(db::TUPLE_SIZE);
            
            for _ in 0..(db::TUPLE_SIZE/32) {
                raw_tuple.extend_from_slice(&rand::random::<[u8; 32]>()[..]);
            }

            raw_tuple.extend_from_slice(&rand::random::<[u8; db::TUPLE_SIZE % 32]>()[..]);
            worker_db.push(Rc::new(db::PungTuple::new(&raw_tuple[..])));
        }

        if workers > 1 {
            worker_db.sort();
        }

        set.extend_from_slice(&worker_db[..]);
    }
}


#[test]
fn db_sort_500k() {
    fn db_sort_500k(b: &mut Bencher) {
        let len = 500000;
        let workers = 1;

        let mut set = Vec::with_capacity(len * workers);
        create_db(len, workers, &mut set); 

        b.iter_with_setup(|| set.clone(), |mut data| quicksort::quicksort(&mut data));
    }
    
    let mut bmark = bmark_settings!();
    bmark.bench_function("db_sort_500k", db_sort_500k);
}

#[test]
fn db_sort_50k() {
    fn db_sort_50k(b: &mut Bencher) {
        let len = 50000;
        let workers = 1;

        let mut set = Vec::with_capacity(len * workers);
        create_db(len, workers, &mut set); 

        b.iter_with_setup(|| set.clone(), |mut data| quicksort::quicksort(&mut data));
    }

    let mut bmark = bmark_settings!();
    bmark.bench_function("db_sort_50k", db_sort_50k);
}

#[test]
fn db_bst_500k() {
    fn db_bst_500k(b: &mut Bencher) {
        let len = 500000;
        let workers = 1;

        let mut set = Vec::with_capacity(len * workers);
        create_db(len, workers, &mut set); 
        set.sort();

        b.iter_with_setup(|| set.clone(), |mut data| data.as_bst_order());
    }
    
    let mut bmark = bmark_settings!();
    bmark.bench_function("db_bst_500k", db_bst_500k);
}

#[test]
fn db_bst_50k() {
    fn db_bst_50k(b: &mut Bencher) {
        let len = 50000;
        let workers = 1;

        let mut set = Vec::with_capacity(len * workers);
        create_db(len, workers, &mut set); 
        set.sort();

        b.iter_with_setup(|| set.clone(), |mut data| data.as_bst_order());
    }

    let mut bmark = bmark_settings!();
    bmark.bench_function("db_bst_50k", db_bst_50k);
}

#[test]
fn db_sort_bst_500k() {
    fn db_sort_bst_500k(b: &mut Bencher) {
        let len = 500000;
        let workers = 1;

        let mut set = Vec::with_capacity(len * workers);
        create_db(len, workers, &mut set); 

        b.iter_with_setup(|| set.clone(), |mut data| { 
            quicksort::quicksort(&mut data); 
            data.as_bst_order(); 
        });
    }

    let mut bmark = bmark_settings!();
    bmark.bench_function("db_sort_bst_500k", db_sort_bst_500k);
}

#[test]
fn db_sort_bst_50k() {
    fn db_sort_bst_50k(b: &mut Bencher) {
        let len = 50000;
        let workers = 1;

        let mut set = Vec::with_capacity(len * workers);
        create_db(len, workers, &mut set); 

        b.iter_with_setup(|| set.clone(), |mut data| { 
            quicksort::quicksort(&mut data);
            data.as_bst_order() 
        });
    }

    let mut bmark = bmark_settings!();
    bmark.bench_function("db_sort_bst_50k", db_sort_bst_50k);
}
