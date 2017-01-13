extern crate capnpc;
extern crate gcc;
extern crate cmake;

fn main() {

    // Compile RPC schema file into rust code
    capnpc::CompilerCommand::new()
        .src_prefix("schema")
        .file("schema/pung.capnp")
        .run().expect("schema compiler command");


    // Compile and link pung C++ PIR shim
    gcc::Config::new()
                 .file("src/pir/cpp/pungPIR.cpp")
                 .include("deps/xpir/")
                 .flag("-std=c++11")
                 .flag("-fopenmp")
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
}
