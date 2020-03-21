use libc;
use std::slice;

use super::{PirQuery, PirResult};

// Functions from C++ shim
// #[link(name = "gomp")]
// #[link(name = "gmp")]
// #[link(name = "mpfr")]
// #[link(name = "boost_thread")]
// #[link(name = "boost_system")]
extern "C" {
    fn cpp_client_setup(
        len: u64,
        num: u64,
        alpha: u64,
        depth: u64,
    ) -> *mut libc::c_void;

    fn cpp_client_generate_query(
        client: *const libc::c_void,
        index: u64,
        q_len: *mut u64,
        q_num: *mut u64,
    ) -> *mut u8;

    fn cpp_client_process_reply(
        client: *const libc::c_void,
        answer: *const u8,
        a_len: u64,
        a_num: u64,
        r_len: *mut u64,
    ) -> *mut u8;

    fn cpp_client_free(client: *mut libc::c_void);

    fn cpp_client_update_db_params(
        client: *const libc::c_void,
        len: u64,
        num: u64,
        alpha: u64,
        depth: u64,
    );
}


pub struct PirClient<'a> {
    client: &'a mut libc::c_void,
    depth: u64,
}

impl<'a> Drop for PirClient<'a> {
    fn drop(&mut self) {
        unsafe {
            cpp_client_free(self.client);
        }
    }
}

impl<'a> PirClient<'a> {
    pub fn new(size: u64, num: u64, alpha: u64, depth: u64) -> PirClient<'a> {
        let client_ptr: &'a mut libc::c_void =
            unsafe { &mut *(cpp_client_setup(size * num, num, alpha, depth)) };

        PirClient {
            client: client_ptr,
            depth: depth,
        }
    }

    pub fn update_params(&self, size: u64, num: u64, alpha: u64) {
        unsafe {
            cpp_client_update_db_params(self.client, size * num, num, alpha, self.depth);
        }
    }

    pub fn gen_query(&self, index: u64) -> PirQuery<'a> {
        let mut q_len: u64 = 0;
        let mut q_num: u64 = 0;

        let query: &'a mut [u8] = unsafe {
            let ptr = cpp_client_generate_query(self.client, index, &mut q_len, &mut q_num);
            slice::from_raw_parts_mut(ptr as *mut u8, q_len as usize)
        };

        PirQuery {
            query: query,
            num: q_num,
        }
    }


    pub fn decode_answer(&self, answer: &[u8], a_num: u64) -> PirResult<'a> {
        let mut r_len: u64 = 0;

        let result: &'a mut [u8] = unsafe {
            let ptr = cpp_client_process_reply(
                self.client,
                answer.as_ptr(),
                answer.len() as u64,
                a_num,
                &mut r_len,
            );
            slice::from_raw_parts_mut(ptr as *mut u8, r_len as usize)
        };

        PirResult { result: result }
    }
}
