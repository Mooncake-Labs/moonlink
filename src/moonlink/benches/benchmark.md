# Moonlink benchmarks

## Overview
Moonlink includes several microbenchmarks to measure performance of core components.

## Run Benchmarks
 ````
 cargo bench --features='bench'

 cargo bench --bench microbench_write_mooncake_table  --features='bench'
````

## Run with profiling

 ````
 cargo bench --bench microbench_write_mooncake_table  --features='bench'
 -- --profile-time=5
 ````

then find flamegraph in target/criterion/TEST_NAME/profile