import asyncio
from math import isfinite
from queue import Empty, Queue
from threading import Thread

from ..data_structures import DataUpdateDetails
from .codec import decode_update, encode_update


class NatsDataFabricTransport:
    """Core NATS broadcast notifications with synchronous session-facing methods.

    A private asyncio loop receives messages while the session loads storage.
    start() flushes SUB before returning. No queue group or reconnect is used:
    a disconnect or slow-consumer error invalidates notification continuity.
    connect_options accepts nats-py authentication/TLS and connection options.
    """

    def __init__(self, servers: str | list[str], subject: str, *, connect_options=None,
                 timeout: float = 10.0):
        if not isfinite(timeout) or timeout <= 0:
            raise ValueError("Operation timeout must be finite and positive")
        if not subject or any(c.isspace() for c in subject) or any(
            not token or "*" in token or ">" in token for token in subject.split(".")
        ):
            raise ValueError("Use one concrete NATS subject for publishing and receiving")
        self._servers = servers
        self._subject = subject
        self._options = dict(connect_options or {})
        self._timeout = timeout
        self._loop = None
        self._thread = None
        self._client = None
        self._messages = Queue()
        self._error = None
        self._closing = False

    def start(self) -> None:
        if self._loop is not None:
            raise RuntimeError("Transport is already started")
        self._messages = Queue()
        self._error = None
        self._closing = False
        self._loop = asyncio.new_event_loop()
        self._thread = Thread(target=self._run_loop, name="fabric-nats")
        self._thread.start()
        try:
            self._call(self._start())
            self._check()
        except BaseException:
            self.stop()
            raise

    def _run_loop(self):
        asyncio.set_event_loop(self._loop)
        try:
            self._loop.run_forever()
        finally:
            pending = asyncio.all_tasks(self._loop)
            for task in pending:
                task.cancel()
            if pending:
                self._loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))
            self._loop.close()

    async def _start(self):
        from nats.aio.client import Client

        self._client = Client()
        await self._client.connect(servers=self._servers, **(self._options | {
            "allow_reconnect": False,
            "no_echo": False,
            "error_cb": self._on_error,
            "disconnected_cb": self._on_disconnect,
        }))
        await self._client.subscribe(self._subject, cb=self._receive)
        # PONG confirms the server processed our preceding SUB. Messages are
        # queued by the client loop even before the session first calls poll().
        await self._client.flush(timeout=self._timeout)

    async def _receive(self, message):
        self._messages.put(message.data)

    async def _on_error(self, error):
        if not self._closing and self._error is None:
            self._error = error
            self._messages.put(error)  # Wake an indefinitely blocked poll.

    async def _on_disconnect(self):
        await self._on_error(ConnectionError("NATS disconnected; start a new session with a fresh cache"))

    def _call(self, coroutine):
        future = asyncio.run_coroutine_threadsafe(coroutine, self._loop)
        try:
            return future.result(timeout=self._timeout)
        except BaseException:
            future.cancel()
            raise

    def _check(self):
        if self._loop is None or self._client is None:
            raise RuntimeError("Transport is not started")
        if self._error is not None:
            raise self._error

    def stop(self) -> None:
        if self._loop is None:
            return
        self._closing = True
        try:
            if self._client is not None:
                self._call(self._client.close())
        finally:
            self._loop.call_soon_threadsafe(self._loop.stop)
            self._thread.join()
            self._loop = self._thread = self._client = None

    def poll(self, timeout: float | None = None) -> DataUpdateDetails | None:
        if timeout is not None and (not isfinite(timeout) or timeout < 0):
            raise ValueError("Poll timeout must be finite and nonnegative")
        self._check()
        try:
            payload = self._messages.get(timeout=timeout)
        except Empty:
            self._check()
            return None
        self._check()
        if isinstance(payload, Exception):
            raise payload
        return decode_update(payload)

    def send(self, update: DataUpdateDetails) -> None:
        self._check()
        payload = encode_update(update)

        async def publish():
            await self._client.publish(self._subject, payload)
            await self._client.flush(timeout=self._timeout)

        self._call(publish())
        self._check()
