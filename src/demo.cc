#include "DB4NFV/include/ffi.h"
#include <iostream>

std::string Init_SFC(int argc, std::vector<std::string> argv)
{
	std::cout << "Init_SFC called with argc: " << argc << " and argv:";
	for (const auto &arg : argv)
	{
		std::cout << " " << arg;
	}
	std::cout << std::endl;
	return ""; // Returning empty string as default value
}

void VNFThread(int c, std::vector<std::string> v)
{
	std::cout << "VNFThread called with c: " << c << " and v:";
	for (const auto &str : v)
	{
		std::cout << " " << str;
	}
	std::cout << std::endl;
}

std::vector<uint8_t> execute_sa_udf(uint64_t txnReqId_jni, int saIdx, std::vector<uint8_t> value, int param_count)
{
	std::cout << "execute_sa_udf called with txnReqId_jni: " << txnReqId_jni << ", saIdx: " << saIdx << ", value size: " << value.size() << ", param_count: " << param_count << std::endl;
	return std::vector<uint8_t>(); // Returning empty vector as default value
}

int txn_finished(uint64_t txnReqId_jni)
{
	std::cout << "txn_finished called with txnReqId_jni: " << txnReqId_jni << std::endl;
	return 0; // Returning 0 as default value
}