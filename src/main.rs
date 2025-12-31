use actix_web::{get, App, HttpResponse, HttpServer, Responder};
use anyhow::Result;
use clap::Parser;
use prometheus::{self, gather, Encoder, TextEncoder};
use tokio::signal;

use redis_proxy::metrics::prometheus::update_process_metrics;

use redis_proxy::proxy::node::RedisNode;
use redis_proxy::proxy::proxy::RedisProxy;

/// Redis proxy CLI that accepts multiple node IDs and hosts
#[derive(Parser, Debug)]
#[command(author, version, about)]
struct Args {
    /// List of node IDs (e.g. --node-ids node1 node2 node3)
    #[arg(long, num_args = 1.., value_name = "NODE_ID")]
    node_ids: Vec<String>,

    /// List of hosts (e.g. --hosts 127.0.0.1:6379 127.0.0.1:6380)
    #[arg(long, num_args = 1.., value_name = "HOST")]
    hosts: Vec<String>,

    // Address to listen on (e.g. --address 0.0.0.0:6379)
    #[arg(long, value_name = "ADDRESS", default_value = "0.0.0.0:6379")]
    address: String,
}

#[get("/metrics")]
async fn metrics() -> impl Responder {
    let encoder = TextEncoder::new();
    let metric_families = gather();
    let mut buffer = Vec::new();
    encoder.encode(&metric_families, &mut buffer).unwrap();
    HttpResponse::Ok()
        .content_type(encoder.format_type())
        .body(buffer)
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    if args.node_ids.len() != args.hosts.len() {
        eprintln!("Error: node_ids and hosts must have the same length");
        std::process::exit(1);
    }

    // Spawn a background task to refresh system stats
    tokio::spawn(async {
        loop {
            update_process_metrics();
            tokio::time::sleep(std::time::Duration::from_secs(5)).await;
        }
    });

    let mut nodes: Vec<RedisNode> = Vec::new();

    for (id, host) in args.node_ids.iter().zip(args.hosts.iter()) {
        let parts: Vec<&str> = host.split(':').collect();
        let host = parts[0].to_string();
        let port = parts[1].parse::<u16>().unwrap_or(6379);

        println!("Node {id} -> {host}:{port}");
        nodes.push(RedisNode::new(id.clone(), host, port));
    }

    let proxy = RedisProxy::new(150, 50);

    for node in nodes {
        if let Err(e) = proxy.add_node(node.clone()).await {
            eprintln!("Failed to add node {}: {}", node.id, e);
        } else {
            println!(
                "Added Redis node: {} at {}:{}",
                node.id, node.host, node.port
            );
        }
    }

    // Start metrics server in background
    let metrics_server = HttpServer::new(|| App::new().service(metrics))
        .bind("127.0.0.1:9090")?
        .run();

    // Start Redis proxy server in background
    let proxy_server = proxy.start_server(&args.address);

    // Graceful shutdown on Ctrl+C
    tokio::select! {
        res = metrics_server => {
            if let Err(e) = res {
                eprintln!("Metrics server error: {}", e);
            }
        }
        res = proxy_server => {
            if let Err(e) = res {
                eprintln!("Proxy server error: {}", e);
            }
        }
        _ = signal::ctrl_c() => {
            println!("Received Ctrl+C, shutting down...");
        }
    }

    Ok(())
}
