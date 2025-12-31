use lazy_static::lazy_static;

use prometheus::{self};
use prometheus::{
    register_histogram, register_int_counter, register_int_gauge, Histogram, IntCounter, IntGauge,
};
use sysinfo::System;

lazy_static! {
    pub static ref COMMANDS_PROXIED_COUNTER: IntCounter = register_int_counter!(
        "redis_proxy_commands_proxied_total",
        "Number of commands proxied"
    )
    .unwrap();
    pub static ref CONNECTIONS_GAUGE: IntGauge =
        register_int_gauge!("redis_proxy_connections", "Current number of connections").unwrap();
    pub static ref PROXY_LATENCY_HISTOGRAM: Histogram = register_histogram!(
        "redis_proxy_proxy_latency_seconds",
        "Histogram of proxy latencies in seconds",
        vec![
            0.000001, 0.000002, 0.000005, 0.00001, 0.00002, 0.00005, 0.0001, 0.0002, 0.0005, 0.005,
            0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0
        ]
    )
    .unwrap();
    pub static ref CPU_USAGE_GAUGE: IntGauge =
        register_int_gauge!("process_cpu_usage_percent", "Current process CPU usage (%)").unwrap();
    pub static ref MEMORY_USAGE_GAUGE: IntGauge = register_int_gauge!(
        "process_memory_usage_bytes",
        "Current process memory usage in bytes"
    )
    .unwrap();
    pub static ref SYSTEM_MEMORY_GAUGE: IntGauge =
        register_int_gauge!("system_memory_used_bytes", "System memory used in bytes").unwrap();
    pub static ref SYSTEM_CPU_GAUGE: IntGauge =
        register_int_gauge!("system_cpu_usage_percent", "System-wide CPU usage (%)").unwrap();
}

pub fn update_process_metrics() {
    let mut sys = System::new_all();
    sys.refresh_all();

    if let Some(proc) = sys.process(sysinfo::get_current_pid().unwrap()) {
        CPU_USAGE_GAUGE.set(proc.cpu_usage() as i64);
        MEMORY_USAGE_GAUGE.set(proc.memory() as i64);
    }

    SYSTEM_MEMORY_GAUGE.set(sys.used_memory() as i64);
    SYSTEM_CPU_GAUGE.set(sys.global_cpu_usage() as i64);
}
