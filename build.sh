#!/bin/bash -e
cd runtime
rm -dfr build  \
	&& mkdir build \
	&& cd build
cmake .. -DSTACK=KERNEL -DCMAKE_BUILD_TYPE=Debug \
	&& make \
	&& sudo make install
cd /home/kailian/DB4NFV/runtime/vnf/SL \
	&& make kernel-dynamic