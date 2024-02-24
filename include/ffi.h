#pragma once
#include "rust/cxx.h"
#include "DB4NFV/src/external/ffi.rs.h"
#include <vector>
#include <string>

extern "C++" {
    rust::String Init_SFC(int32_t argc, rust::Vec<rust::String> argv);
    void VNFThread(int32_t c, rust::Vec<rust::String> v);
    rust::String execute_sa_udf(int64_t txnReqId_jni, int saIdx, rust::Vec<uint8_t> value, int param_count);
    int32_t txn_finished(int64_t txnReqId_jni);
}
