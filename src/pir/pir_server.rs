use libc;
use std::mem;
use std::slice;
use super::PirAnswer;

// functions from C++ PungPIR shim
//#[link(name = "gomp")]
//#[link(name = "gmp")]
//#[link(name = "mpfr")]
//#[link(name = "boost_thread")]
//#[link(name = "boost_system")]
extern "C" {
    fn cpp_server_setup(
        len: libc::uint64_t,
        collection: *const libc::uint8_t,
        num: libc::uint64_t,
        alpha: libc::uint64_t,
        depth: libc::uint64_t,
    ) -> *mut libc::c_void;

    fn cpp_server_process_query(
        server: *const libc::c_void,
        q: *const libc::uint8_t,
        q_len: libc::uint64_t,
        q_num: libc::uint64_t,
        a_len: *mut libc::uint64_t, // answer length
        a_num: *mut libc::uint64_t,
    ) -> *mut libc::uint8_t;

    fn cpp_server_free(server: *mut libc::c_void);
}

pub struct PirServer<'a> {
    server: &'a mut libc::c_void,
}

impl<'a> Drop for PirServer<'a> {
    fn drop(&mut self) {
        unsafe {
            cpp_server_free(self.server);
        }
    }
}

impl<'a> PirServer<'a> {
    pub fn new<T>(collection: &[T], alpha: u64, depth: u64) -> PirServer<'a> {
        let server_ptr: &'a mut libc::c_void = unsafe {
            &mut *(cpp_server_setup(
                (collection.len() * mem::size_of::<T>()) as u64,
                collection.as_ptr() as *const u8,
                collection.len() as u64,
                alpha,
                depth,
            ))
        };

        PirServer { server: server_ptr }
    }

    pub fn gen_answer(&self, query: &[u8], q_num: u64) -> PirAnswer<'a> {
        let mut a_len: u64 = 0;
        let mut a_num: u64 = 0;

        let answer: &'a mut [u8] = unsafe {
            let ptr = cpp_server_process_query(
                self.server,
                query.as_ptr(),
                query.len() as u64,
                q_num,
                &mut a_len,
                &mut a_num,
            );
            slice::from_raw_parts_mut(ptr as *mut u8, a_len as usize)
        };

        PirAnswer {
            answer: answer,
            num: a_num,
        }
    }
}
