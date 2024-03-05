// use cxx_build::CFG;

fn main() {
    // Pre compile header file for rust interface.
    cxx_build::bridge("src/external/ffi.rs").compile("morph-db-temp");  // returns a cc::Build
    // Header saved to target/cxxbridge/DB4NFV/src/external/ffi.rs.h

    // Compile vnf runtime with rust lib.
    cc::Build::new()
        .compiler("g++")
        .include("include/")
        .include("../")
        .include("runtime/include/")
        .include("runtime/src/kernel/")
        .include("target/cxxbridge/")
        .flag("-std=c++11")
        .flag("-Wno-deprecated-declarations")
        .flag("-Wno-pointer-arith" )
        .flag("-lnuma" )
        // .flag("-lsctp" )
        .flag("-lrt" )
        .flag("-lpthread" )
        .flag("-lboost-system" )
        .flag("-DLIBVNF_STACK=1" )
        // If debug.
        .flag("-g" )
        .flag("-O0")
        // .flag("-O3")

        // Runtime files.
        .file("runtime/src/kernel/core.cpp")
        .file("runtime/src/datastore/dspackethandler.cpp")
        .file("runtime/src/datastore/utils.cpp")
        // VNF app files.
        .file("runtime/vnf/SL/sl.cpp")
        // .shared_flag(true)
        // .out_dir("./")
        .compile("morph-db");

    println!("cargo:rerun-if-changed=include/ffi.h");
    println!("cargo:rerun-if-changed=runtime/core.hpp");
    println!("cargo:rerun-if-changed=runtime/src/kernel/core.cpp");
    println!("cargo:rerun-if-changed=runtime/src/kernel/utils.hpp");
    println!("cargo:rerun-if-changed=runtime/vnf/SL/sl.cpp");
}