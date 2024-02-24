fn main() {
    cxx_build::bridge("src/external/ffi.rs")  // returns a cc::Build
        .file("src/demo.cc")
        .flag_if_supported("-std=c++11")
        .compile("morph-db");

    println!("cargo:rerun-if-changed=src/external/ffi.rs");
    println!("cargo:rerun-if-changed=src/demo.cc");
    println!("cargo:rerun-if-changed=include/demo.h");
}