extern crate capnpc;
extern crate cc;
extern crate cmake;

fn main() {

    // Compile RPC schema file into rust code
    capnpc::CompilerCommand::new()
        .src_prefix("schema")
        .file("schema/pung.capnp")
        .run().expect("schema compiler command");


    // Compile and link pung C++ PIR shim
    cc::Build::new()
                 .file("src/pir/cpp/pungPIR.cpp")
                 .include("deps/xpir/")
                 .flag("-std=c++11")
                 .flag("-fopenmp")
                 .flag("-Wno-unused-parameter")
                 .pic(true)
                 .cpp(true)
                 .compile("libpung_pir.a");

    // Compile and link XPIR c++ shim
    let dst = cmake::Config::new("deps/xpir")
                             .define("CMAKE_BUILD_TYPE", "Release")
                             .define("MULTI_THREAD", "OFF")
                             .define("PERF_TIMERS", "OFF")
                             .build();

    println!("cargo:rustc-link-search=native={}/build/pir", dst.display());
    println!("cargo:rustc-link-lib=static=pir_static");

    // Dynamic libraries needed by XPIR
    println!("cargo:rustc-link-lib=gomp");
    println!("cargo:rustc-link-lib=gmp");
    println!("cargo:rustc-link-lib=mpfr");
    println!("cargo:rustc-link-lib=boost_thread");
    println!("cargo:rustc-link-lib=boost_system");
}
