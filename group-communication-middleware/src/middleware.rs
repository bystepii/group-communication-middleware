use std::{
    collections::HashMap,
    ops::Range,
    sync::{
        mpsc::{self, Receiver, Sender},
        Arc, Mutex,
    },
};

use futures::{FutureExt, StreamExt};
use lapin::{
    message::Delivery,
    options::{
        BasicAckOptions, BasicPublishOptions, ExchangeDeclareOptions, QueueBindOptions,
        QueueDeclareOptions,
    },
    types::{AMQPValue, FieldTable},
    BasicProperties, Channel, Connection, ConnectionProperties, Consumer, ExchangeKind,
};

use crate::impl_chainable_setter;
use crate::types::Message;

type Error = Box<dyn std::error::Error + Send + Sync>;
type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Clone)]
pub struct MiddlewareArguments {
    rabbitmq_uri: String,
    exchange_name: String,
    queue_prefix: String,
    global_range: Range<u32>,
    local_range: Range<u32>,
}

impl MiddlewareArguments {
    pub fn new(rabbitmq_uri: String, global_range: Range<u32>, local_range: Range<u32>) -> Self {
        Self {
            rabbitmq_uri,
            exchange_name: "exchange".to_string(),
            queue_prefix: "queue".to_string(),
            global_range,
            local_range,
        }
    }

    impl_chainable_setter! {
        exchange_name, String
    }
    impl_chainable_setter! {
        queue_prefix, String
    }
}

#[derive(Clone)]
pub struct Middleware {
    options: MiddlewareArguments,
    rabbitmq_conn: Arc<Connection>,
    rabbitmq_channel: Option<Channel>,

    local_queues: HashMap<u32, String>,

    local_channel_tx: HashMap<u32, Sender<Message>>,
    local_channel_rx: HashMap<u32, Arc<Mutex<Receiver<Message>>>>,

    consumer: Option<Consumer>,

    id: Option<u32>,
}

impl Middleware {
    pub async fn init_global(args: MiddlewareArguments) -> Result<Self> {
        let rabbitmq_conn =
            Connection::connect(&args.rabbitmq_uri, ConnectionProperties::default()).await?;

        // TODO: set publisher_confirms to true
        let channel = rabbitmq_conn.create_channel().await?;

        // Declare exchange
        let mut options = ExchangeDeclareOptions::default();
        options.durable = true;

        channel
            .exchange_declare(
                &args.exchange_name,
                ExchangeKind::Direct,
                options,
                FieldTable::default(),
            )
            .await?;

        // Declare all queues
        let mut options = QueueDeclareOptions::default();
        options.durable = true;

        let mut local_queues = HashMap::new();

        let queues = futures::future::try_join_all(args.global_range.clone().map(|id| {
            channel.queue_declare(
                format!("{}_{}", args.queue_prefix, id).leak(),
                options,
                FieldTable::default(),
            )
        }))
        .await?;

        // Bind queues to exchange
        futures::future::try_join_all(queues.iter().map(|queue| {
            channel.queue_bind(
                queue.name().as_str(),
                &args.exchange_name,
                queue.name().as_str(),
                QueueBindOptions::default(),
                FieldTable::default(),
            )
        }))
        .await?;

        channel.close(200, "Bye").await?;

        // add local queues to local_queues
        for (id, queue) in args.local_range.clone().zip(queues) {
            local_queues.insert(id, queue.name().to_string());
        }

        // create local channels
        let mut local_channel_tx = HashMap::new();
        let mut local_channel_rx = HashMap::new();

        for id in args.local_range.clone() {
            let (tx, rx) = mpsc::channel::<Message>();
            local_channel_tx.insert(id, tx);
            local_channel_rx.insert(id, Arc::new(Mutex::new(rx)));
        }

        Ok(Self {
            options: args,
            rabbitmq_conn: Arc::new(rabbitmq_conn),
            rabbitmq_channel: None,
            local_queues,
            local_channel_tx,
            local_channel_rx,
            consumer: None,
            id: None,
        })
    }

    pub async fn init_local(&mut self, id: u32) -> Result<()> {
        self.rabbitmq_channel = Some(self.rabbitmq_conn.create_channel().await?);
        self.id = Some(id);
        self.consumer = Some(
            self.rabbitmq_channel
                .as_ref()
                .unwrap()
                .basic_consume(
                    &self.local_queues.get(&self.id.unwrap()).unwrap(),
                    &format!(
                        "{}_{} consumer",
                        self.options.queue_prefix,
                        self.id.unwrap()
                    ),
                    lapin::options::BasicConsumeOptions::default(),
                    FieldTable::default(),
                )
                .await?,
        );
        Ok(())
    }

    pub async fn send(&self, dest: u32, data: Vec<u8>) -> Result<()> {
        let chunk_id = 0;
        let last_chunk = true;

        if self.rabbitmq_channel.is_none() | self.id.is_none() {
            return Err("init_local() must be called before send()".into());
        }

        if !self.options.global_range.contains(&dest) {
            return Err("worker with id {} does not exist".into());
        }
        if self.options.local_range.contains(&dest) {
            if let Some(tx) = self.local_channel_tx.get(&dest) {
                tx.send(Message {
                    sender_id: self.id.unwrap(),
                    data,
                    chunk_id,
                    last_chunk,
                })?
            } else {
                return Err("worker with id {} has no channel registered".into());
            }
        } else {
            let mut fields = FieldTable::default();
            fields.insert("sender_id".into(), AMQPValue::LongUInt(self.id.unwrap()));
            fields.insert("chunk_id".into(), AMQPValue::LongUInt(chunk_id));
            fields.insert("last_chunk".into(), AMQPValue::Boolean(last_chunk));

            self.rabbitmq_channel
                .as_ref()
                .unwrap()
                .basic_publish(
                    &self.options.exchange_name,
                    &format!("{}_{}", self.options.queue_prefix, dest),
                    BasicPublishOptions::default(),
                    &data,
                    BasicProperties::default().with_headers(fields),
                )
                .await?;
        }
        Ok(())
    }

    pub async fn try_rcv(&self) -> Result<Option<Message>> {
        if self.rabbitmq_channel.is_none() || self.id.is_none() || self.consumer.is_none() {
            return Err("init_local() must be called before receive()".into());
        }

        if let Some(rx) = self.local_channel_rx.get(&self.id.unwrap()) {
            // try receive without blocking
            if let Ok(msg) = rx.lock().unwrap().try_recv() {
                return Ok(Some(msg));
            }
        }

        // try receive without blocking
        if let Some(delivery) = self
            .consumer
            .clone()
            .unwrap()
            .next()
            .now_or_never()
            .flatten()
        {
            let delivery = delivery?;
            delivery.ack(BasicAckOptions::default()).await?;
            return Ok(Some(Self::get_message(delivery)));
        }

        Ok(None)
    }

    pub async fn recv(&self) -> Result<Message> {
        if self.rabbitmq_channel.is_none()
            || self.id.is_none()
            || self.consumer.is_none()
            || !self.local_channel_rx.contains_key(&self.id.unwrap())
        {
            return Err("init_local() must be called before receive()".into());
        }

        let rx = self
            .local_channel_rx
            .get(&self.id.unwrap())
            .unwrap()
            .clone();

        // receive blocking
        let handle = tokio::task::spawn_blocking(move || rx.lock().unwrap().recv());

        // Receive from rabbitmq
        let fut = async {
            // receive blocking
            let delivery = self.consumer.clone().unwrap().next().await.unwrap()?;
            delivery.ack(BasicAckOptions::default()).await?;
            Ok::<Message, Error>(Self::get_message(delivery))
        };

        tokio::select! {
            msg = fut => Ok(msg?),
            msg = handle => Ok(msg??),
        }
    }

    fn get_message(delivery: Delivery) -> Message {
        let data = delivery.data;
        let sender_id = delivery
            .properties
            .headers()
            .as_ref()
            .unwrap()
            .inner()
            .get("sender_id")
            .unwrap()
            .as_long_uint()
            .unwrap();
        let chunk_id = delivery
            .properties
            .headers()
            .as_ref()
            .unwrap()
            .inner()
            .get("chunk_id")
            .unwrap()
            .as_long_uint()
            .unwrap();
        let last_chunk = delivery
            .properties
            .headers()
            .as_ref()
            .unwrap()
            .inner()
            .get("last_chunk")
            .unwrap()
            .as_bool()
            .unwrap();
        Message {
            sender_id,
            data,
            chunk_id,
            last_chunk,
        }
    }
}
