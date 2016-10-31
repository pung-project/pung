extern crate pung;
extern crate rand;

use pung::db;
use pung::db::bst::BSTOrder;
use rand::ChaChaRng;
use rand::Rng;


#[test]
fn bst_to_arr() {
    let mut input = vec![1, 2, 3, 4, 5];
    let correct = vec![4, 2, 5, 1, 3];
    input.as_bst_order();
    assert_eq!(correct, input);


    let mut input = vec![1];
    let correct = vec![1];
    input.as_bst_order();
    assert_eq!(correct, input);

    let mut input = vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14];
    let correct = vec![8, 4, 12, 2, 6, 10, 14, 1, 3, 5, 7, 9, 11, 13];
    input.as_bst_order();
    assert_eq!(correct, input);
}

fn create_tuples(num: usize, set: &mut Vec<db::PungTuple>, label_hack: Option<u8>){

    let mut rng = ChaChaRng::new_unseeded();

    for _ in 0..num {
        let mut raw_tuple = [0u8; db::TUPLE_SIZE];
        rng.fill_bytes(&mut raw_tuple);

        if label_hack.is_some() {
            raw_tuple[0] = label_hack.unwrap();
            raw_tuple[7] = label_hack.unwrap();
        }

        set.push(db::PungTuple::new(&raw_tuple[..]));
    }
}



#[test]
fn batch_code_2_explicit() {
    let num = 1000;

    let mut tuples_1 = Vec::with_capacity(num);
    let mut tuples_2 = Vec::with_capacity(num);
    create_tuples(num, &mut tuples_1, Some(0));
    create_tuples(num, &mut tuples_2, Some(255));

    let mut bucket = db::Bucket::new(db::RetScheme::Explicit, db::OptScheme::Hybrid2, 1);
    
    for tuple in &tuples_1 {
        bucket.push(tuple.clone());
    }

    for tuple in &tuples_2 {
        bucket.push(tuple.clone());
    }

    bucket.encode();

    tuples_1.sort();
    tuples_2.sort();
     
    assert!(bucket.len() == 3000);
    assert!(bucket.get_collection(0).len() == 1000);
    assert!(bucket.get_collection(1).len() == 1000);
    assert!(bucket.get_collection(2).len() == 1000);
    assert!(tuples_1[0] == *bucket.get_collection(0).get_tuple(0));
    assert!(tuples_2[0] == *bucket.get_collection(1).get_tuple(0));
    assert!((&tuples_1[0] ^ &tuples_2[0]) == *bucket.get_collection(2).get_tuple(0));

    assert!(tuples_1[50] == *bucket.get_collection(0).get_tuple(50));
    assert!(tuples_2[50] == *bucket.get_collection(1).get_tuple(50));
    assert!((&tuples_1[50] ^ &tuples_2[50]) == *bucket.get_collection(2).get_tuple(50));


    assert!(tuples_1[120] == *bucket.get_collection(0).get_tuple(120));
    assert!(tuples_2[120] == *bucket.get_collection(1).get_tuple(120));
    assert!((&tuples_1[120] ^ &tuples_2[120]) == *bucket.get_collection(2).get_tuple(120));
}

#[test]
fn batch_code_2_bst() {
    let num = 1000;

    let mut tuples_1 = Vec::with_capacity(num);
    let mut tuples_2 = Vec::with_capacity(num);
    create_tuples(num, &mut tuples_1, Some(0));
    create_tuples(num, &mut tuples_2, Some(255));

    let mut bucket = db::Bucket::new(db::RetScheme::Tree, db::OptScheme::Hybrid2, 1);
    
    for tuple in &tuples_1 {
        bucket.push(tuple.clone());
    }

    for tuple in &tuples_2 {
        bucket.push(tuple.clone());
    }

    bucket.encode();

    tuples_1.sort(); 
    tuples_1.as_bst_order();
    tuples_2.sort(); 
    tuples_2.as_bst_order();
     
    assert!(bucket.len() == 3000);
    assert!(bucket.get_collection(0).len() == 1000);
    assert!(bucket.get_collection(1).len() == 1000);
    assert!(bucket.get_collection(2).len() == 1000);
    assert!(tuples_1[0] == *bucket.get_collection(0).get_tuple(0));
    assert!(tuples_2[0] == *bucket.get_collection(1).get_tuple(0));
    assert!((&tuples_1[0] ^ &tuples_2[0]) == *bucket.get_collection(2).get_tuple(0));

    assert!(tuples_1[50] == *bucket.get_collection(0).get_tuple(50));
    assert!(tuples_2[50] == *bucket.get_collection(1).get_tuple(50));
    assert!((&tuples_1[50] ^ &tuples_2[50]) == *bucket.get_collection(2).get_tuple(50));


    assert!(tuples_1[120] == *bucket.get_collection(0).get_tuple(120));
    assert!(tuples_2[120] == *bucket.get_collection(1).get_tuple(120));
    assert!((&tuples_1[120] ^ &tuples_2[120]) == *bucket.get_collection(2).get_tuple(120));
}
