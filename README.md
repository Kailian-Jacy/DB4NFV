# DB4NFV [^1]
A TSPE (Transactional Streaming Processing Engine) Designed for NFV (Network Function Virtualization)

This is a framework for users to develop high-performance, scalable and cross-flow VNFs. It's aimed at using light-weighted ACID transaction to provide robustness and easy coding for complex network programming.

Especially, it's optimized to solve the case VNF needs to heavily read & write cross-flow states that usually require heavy locking mechanism, which could be really slowed down and dangerous for network fundamentals.

By design, it should support any runtime fulfilling the interface. Here we are using modified libVNF as runtime [^2].


# Performance 

Overview:

- Total Latency (Transaction received - Transaction Commit / Aborted)

- Transaction Request handing rates / (second * thread).

![Alt text](assets/image.png)

Stage: Latency for an event finish all its needed states.

![Alt text](assets/state_access.png)

Stage: Handle commitment and abortion of transactions.

![Alt text](assets/transactions.png)


# Installation

Requirements:
- Rust compiler. (Tested on 1.76.0)
- g++ compiler. (Tested on 7.5.0)
- Linux kernel. (Tested on ubuntu 18.04)

Get repository:

```bash
git clone --recurse-submodules https://github.com/Kailian-Jacy/DB4NFV	

cd DB4NFV
```

Compile source:
```bash
cargo build --release
```

Config json configurations to project root directory:
```bash
cat <<EOF > config.json 
{
  "vnf_threads_num": 3,
  "worker_threads_num": 3,
  "waiting_queue_size": 4096,
  "transaction_out_of_order_time_ns": 100,
  "ringbuffer_size": 10000,
  "ringbuffer_full_to_panic": false,
  "transaction_pooling_size": 10000,
  "max_state_records": 10000,
  "verbose": true,
  "monitor_enabled": true,
  "log_dir": "./perf"
}
EOF
```

To run this system, make sure you have
 `$vnf_threads_num + $worker_threads_num + 1 [+1 (if monitor enabled)]` 
 bindable cores. 

Example VNF is under `DB4NFV/runtime/SL` directory. Configuration for such VNF are also under that directory. Like port to expose etc.

```csv
bufferSize,128
dataStoreThreshold,131072
coreNumbers,3
dataStoreSocketPath,/home/kailian/libVNF/vnf/tmp/
dataStoreFileNames,1
dataStoreIP,127.0.0.1
dataStorePort,9090
serverIP,127.0.0.1
serverPort,9090
reuseMode,1
debug,true
monitorInterval,200
```


Run with:
```bash
./target/release/DB4NFV
```

It may take some seconds to spawn.

Send test request from another terminal:
```
python3 ./runtime/vnf/SL/testbench_sender.py
```

# Thinkings During This Project

A doc with development considerations sees: *https://kailian.notion.site/DB4NFV-An-Transactional-Streaming-Processing-Engine-for-NFV-Racing-States-7ac42ee28c7f475897b02626541940cd?pvs=4*

[^1]: Promoted from MorphStream/DB4NFV: *https://github.com/intellistream/MorphStream/tree/DB4NFV*

[^2]: VNF runtime part promoted from libVNF: *https://github.com/networkedsystemsIITB/libVNF*