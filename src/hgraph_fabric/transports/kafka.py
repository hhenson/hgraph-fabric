from math import isfinite
from uuid import uuid4

from ..data_structures import DataUpdateDetails
from .codec import decode_update, encode_update


class KafkaDataFabricTransport:
    """Live notifications on an existing, single-partition Kafka topic.

    Each instance reads independently from the offset captured by start(). No
    consumer-group balancing, committed offsets, or pre-start replay is used.
    config supplies common librdkafka connection/security options to both clients.
    Calls belong to one session thread. Errors require a new session and cache.
    """

    def __init__(self, bootstrap_servers: str, topic: str, *, config=None, timeout: float = 10.0):
        if not isfinite(timeout) or timeout <= 0:
            raise ValueError("Operation timeout must be finite and positive")
        self._config = dict(config or {}) | {"bootstrap.servers": bootstrap_servers}
        self._topic = topic
        self._timeout = timeout
        self._consumer = None
        self._producer = None
        self._error = None

    def start(self) -> None:
        from confluent_kafka import Consumer, KafkaException, Producer, TopicPartition

        if self._consumer is not None:
            raise RuntimeError("Transport is already started")
        self._error = None

        def on_error(error):
            # librdkafka also reports advisory connection/retry events here.
            # Actual delivery and consumption failures are checked below.
            if error.fatal():
                self._error = KafkaException(error)

        try:
            self._consumer = Consumer(self._config | {
                "group.id": f"hgraph-fabric-{uuid4()}",
                "enable.auto.commit": False,
                "enable.auto.offset.store": False,
                "auto.offset.reset": "error",
                "allow.auto.create.topics": False,
                "error_cb": on_error,
            })
            metadata = self._consumer.list_topics(self._topic, timeout=self._timeout)
            topic = metadata.topics[self._topic]
            if topic.error is not None:
                raise KafkaException(topic.error)
            if set(topic.partitions) != {0}:
                raise ValueError("Fabric notifications require a single-partition Kafka topic")
            _, boundary = self._consumer.get_watermark_offsets(
                TopicPartition(self._topic, 0), timeout=self._timeout, cached=False,
            )
            # OFFSET_END is resolved lazily by the consumer and could skip
            # publications made during initialization. Pin a concrete offset now.
            self._consumer.assign([TopicPartition(self._topic, 0, boundary)])
            self._producer = Producer(self._config | {
                "enable.idempotence": True, "acks": "all", "error_cb": on_error,
            })
            self._check()
        except BaseException:
            self.stop()
            raise

    def stop(self) -> None:
        consumer, producer = self._consumer, self._producer
        self._consumer = self._producer = None
        try:
            if consumer is not None:
                consumer.close()
        finally:
            if producer is not None:
                # Successful sends have already flushed. Discard anything left
                # by a failed send rather than publishing it during shutdown.
                producer.purge()
                producer.flush(0)

    def _check(self) -> None:
        if self._consumer is None or self._producer is None:
            raise RuntimeError("Transport is not started")
        if self._error is not None:
            raise self._error

    def poll(self, timeout: float | None = None) -> DataUpdateDetails | None:
        if timeout is not None and (not isfinite(timeout) or timeout < 0):
            raise ValueError("Poll timeout must be finite and nonnegative")
        self._check()
        from confluent_kafka import KafkaException

        message = self._consumer.poll(-1 if timeout is None else timeout)
        self._check()
        if message is None:
            return None
        if message.error() is not None:
            raise KafkaException(message.error())
        return decode_update(message.value())

    def send(self, update: DataUpdateDetails) -> None:
        from confluent_kafka import KafkaException

        self._check()
        errors = []

        def delivered(error, message):
            if error is not None:
                errors.append(KafkaException(error))

        self._producer.produce(
            self._topic, value=encode_update(update), key=update.id.encode("utf-8"),
            partition=0, on_delivery=delivered,
        )
        if self._producer.flush(self._timeout):
            self._error = TimeoutError("Kafka notification delivery timed out")
        elif errors:
            self._error = errors[0]
        self._check()
