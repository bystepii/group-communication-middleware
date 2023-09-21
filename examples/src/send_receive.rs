use std::thread;

use clap::Parser;
use group_communication_middleware::{Middleware, MiddlewareArguments};
use tracing::{error, info};
use tracing_subscriber::{
    fmt, prelude::__tracing_subscriber_SubscriberExt, util::SubscriberInitExt, EnvFilter,
};

type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

#[derive(Parser, Debug)]
pub struct Arguments {
    /// RabbitMQ server address
    #[arg(
        long = "rabbitmq-server",
        default_value = "amqp://guest:guest@localhost:5672",
        required = false
    )]
    pub rabbitmq_server: String,

    /// Start id of the global range
    #[arg(required = true)]
    pub start_global: u32,

    /// End id of the global range (exclusive)
    #[arg(required = true)]
    pub end_global: u32,

    /// Start id of the local range
    #[arg(required = true)]
    pub start_local: u32,

    /// End id of the local range (exclusive)
    #[arg(required = true)]
    pub end_local: u32,
}

#[tokio::main]
async fn main() -> Result<()> {
    // Setup logging

    tracing_subscriber::registry()
        .with(fmt::layer())
        .with(EnvFilter::from_default_env())
        .try_init()?;

    let args = Arguments::parse();

    info!("{:?}", args);

    let mut handles = Vec::new();

    let middleware = match Middleware::init_global(MiddlewareArguments::new(
        args.rabbitmq_server.clone(),
        args.start_global..args.end_global,
        args.start_local..args.end_local,
    ))
    .await
    {
        Ok(m) => m,
        Err(e) => {
            error!("{:?}", e);
            return Ok(());
        }
    };

    for i in args.start_local..args.end_local {
        let mut middleware = middleware.clone();
        handles.push(thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .unwrap();
            rt.block_on(async {
                middleware
                    .init_local(i)
                    .await
                    .expect("Failed to init middleware");

                worker(
                    middleware,
                    i,
                    args.start_global,
                    args.end_global,
                    args.start_local,
                    args.end_local,
                )
                .await
            })
        }));
    }

    for handle in handles {
        if let Err(e) = handle.join().unwrap() {
            error!("{:?}", e);
        }
    }

    Ok(())
}

async fn worker(
    middleware: Middleware,
    id: u32,
    start_global: u32,
    end_global: u32,
    start_local: u32,
    end_local: u32,
) -> Result<()> {
    info!(
        "worker start: id={}, start_global={}, end_global={}, start_local={}, end_local={}",
        id, start_global, end_global, start_local, end_local
    );

    for i in start_global..end_global {
        if i == id {
            continue;
        }
        middleware.send(i, vec![1, 2, 3]).await?;
    }

    for i in start_global..end_global {
        if i == id {
            continue;
        }
        let msg = middleware.recv().await?;
        info!("worker: id={}, recv from id={}", id, msg.sender_id);
        assert_eq!(msg.data, vec![1, 2, 3]);
    }

    Ok(())
}
