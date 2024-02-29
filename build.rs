fn main() {
    cxx_build::bridge("src/external/ffi.rs")  // returns a cc::Build
        .file("runtime/src/core.cpp")
        .include("include/")
        .include("runtime/include/")
        .flag_if_supported("-std=c++11")
        .flag_if_supported("-Wno-deprecated-declarations")
        .flag_if_supported("-Wno-pointer-arith" )
        .flag_if_supported("-lnuma" )
        .flag_if_supported("-lrt" )
        .flag_if_supported("-DLIBVNF_STACK=1" )
        .flag_if_supported("-ljsoncpp" )
        .flag_if_supported("-g" )
        .flag_if_supported("-O0")
        // .flag_if_supported("-3")
        .compile("morph-db");

    println!("cargo:rerun-if-changed=src/external/ffi.rs");
    println!("cargo:rerun-if-changed=runtime/src/kernel/*.cpp");
    println!("cargo:rerun-if-changed=runtime/src/kernel/*.cpp");
}