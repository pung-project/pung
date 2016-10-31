use libc;

// Functions from C++ shim
#[link(name = "gomp")]
#[link(name = "gmp")]
#[link(name = "mpfr")]
#[link(name = "boost_thread")]
#[link(name = "boost_system")]
extern "C" {
    fn cpp_buffer_free(buffer: *mut libc::c_void);
}


pub struct PirQuery<'a> {
    pub query: &'a mut [u8],
    pub num: u64,
}

pub struct PirAnswer<'a> {
    pub answer: &'a mut [u8],
    pub num: u64,
}

pub struct PirResult<'a> {
    pub result: &'a mut [u8],
}


impl<'a> Drop for PirQuery<'a> {
    fn drop(&mut self) {
        unsafe {
            cpp_buffer_free(self.query.as_mut_ptr() as *mut libc::c_void);
        }
    }
}

impl<'a> Drop for PirAnswer<'a> {
    fn drop(&mut self) {
        unsafe {
            cpp_buffer_free(self.answer.as_mut_ptr() as *mut libc::c_void);
        }
    }
}

impl<'a> Drop for PirResult<'a> {
    fn drop(&mut self) {
        unsafe {
            cpp_buffer_free(self.result.as_mut_ptr() as *mut libc::c_void);
        }
    }
}

impl<'a> PirAnswer<'a> {
    pub fn to_bytes(&self) -> Vec<u8> {
        self.answer.to_vec()
    }
}

impl<'a> PirResult<'a> {
    pub fn to_bytes(&self) -> Vec<u8> {
        self.result.to_vec()
    }
}


pub mod pir_client;
pub mod pir_server;
