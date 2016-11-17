"""Thread connection"""
import logging
import socket
import threading

import time

import pika
from pika import frame, exceptions
from pika.adapters.thread_channel import ThreadConnectionChannel
from pika.adapters.thread_connection_io import ThreadConnectionIO
from pika.adapters.transport import Transport
import pika.spec as spec

LOGGER = logging.getLogger(__name__)


class CloseableThread(threading.Thread):
    def __init__(self):
        super(CloseableThread, self).__init__()
        self._stop = threading.Event()

    def stop(self):
        self._stop.set()

    def run(self):
        while not self._stop.isSet():
            self._run0()

    def _run0(self):
        raise NotImplementedError


class HeartbeatSender(CloseableThread):
    def __init__(self, connection):
        super(HeartbeatSender, self).__init__()
        self._connection = connection
        self.last_activity_time = 0
        self.heartbeat = 0
        self.daemon = True

    def _run0(self):
        self._send_heartbeat_frame()

    def signal_activity(self):
        self.last_activity_time = int(round(time.time()))

    def _send_heartbeat_frame(self):
        now = int(round(time.time()))
        if now > (self.last_activity_time + self.heartbeat):
            LOGGER.debug("Sending heartbeat frame")
            self._connection.send_frame(frame.Heartbeat())

    def setHeartbeat(self, heartbeat):
        self.heartbeat = heartbeat


class ThreadConnectionMainLoop(CloseableThread):
    def __init__(self, connection):
        super(ThreadConnectionMainLoop, self).__init__()
        self._connection = connection

    def _run0(self):
        (_frame_type, _channel, _payload, read_frame_buffer) = self._connection.read_frame()
        _consumed_count, frame_value = frame.decode_frame(read_frame_buffer)
        if isinstance(frame_value, frame.Heartbeat):
            LOGGER.debug("receive heartbeat")
        else:
            self._connection.deliver_frame(frame_value)


class ThreadConnection(ThreadConnectionIO):
    def __init__(self, parameters=None):
        super(ThreadConnection, self).__init__(self)
        self.parameters = parameters
        self.server_properties = None
        self.client_properties = {
            'product': pika.connection.PRODUCT,
            'platform': 'Python %s' % pika.connection.platform.python_version(),
            'capabilities': {
                'authentication_failure_close': True,
                'basic.nack': True,
                'connection.blocked': True,
                'consumer_cancel_notify': True,
                'publisher_confirms': True
            },
            'information': 'See http://pika.rtfd.org',
            'version': pika.connection.__version__
        }
        if parameters.client_properties:
            self.client_properties.update(parameters.client_properties)

        self.channels = dict()
        self.channel_number0 = 0

        self._transport = Transport(parameters.host, parameters.port, parameters.socket_timeout,
                                    parameters.ssl,
                                    sslopts=parameters.ssl_options)
        self._try_connect()
        self._heartbeat = HeartbeatSender(self)
        # hanshake
        self._main_loop = ThreadConnectionMainLoop(self)
        self._main_loop.start()
        self._handshake()

        # start heartbeat
        self._heartbeat.setHeartbeat(self.heartbeat)
        self._heartbeat.start()

    def _handshake(self):
        connection_start = self.rpc(self.channel_number0, frame.ProtocolHeader(), [spec.Connection.Start])
        self.server_properties = connection_start.server_properties
        self.publisher_confirms = self.server_properties['capabilities']['publisher_confirms']
        self.basic_nack = self.server_properties['capabilities']['basic.nack']
        (auth_type, response) = self.parameters.credentials.response_for(connection_start)
        tune = self.rpc(self.channel_number0, spec.Connection.StartOk(self.client_properties, auth_type, response,
                                                                      self.parameters.locale), [spec.Connection.Tune])
        self.channel_max = tune.channel_max or self.parameters.channel_max
        self.frame_max = tune.frame_max or self.parameters.frame_max
        self.heartbeat = min(tune.heartbeat, self.parameters.heartbeat)

        self.send_method(self.channel_number0, spec.Connection.TuneOk(self.channel_max, self.frame_max,
                                                                      self.heartbeat))
        connection_open = self.rpc(self.channel_number0, spec.Connection.Open(self.parameters.virtual_host, '', False),
                                   [spec.Connection.OpenOk])
        print connection_open

    def channel(self, channel_number=None):
        if channel_number is None:
            channel_number = self._next_channel_number()
        self.channels[channel_number] = ThreadConnectionChannel(self, channel_number)
        self.channels[channel_number].open()
        return self.channels[channel_number]

    def deliver_frame(self, frame_value):
        if frame_value.channel_number == 0:
            self.handle_frame(frame_value)
            return
        if frame_value.channel_number not in self.channels:
            LOGGER.critical("Received %s frame for unregistered channel %i on %s", frame_value.NAME,
                            frame_value.channel_number, self)
            return
        self.channels[frame_value.channel_number].handle_frame(frame_value)

    def read_frame(self):
        if self.is_closed():
            LOGGER.error("Attempted to read frame when closed")
        return self._transport.read_frame()

    def send_method(self, channel_number, method):
        if isinstance(method, frame.ProtocolHeader):
            self.send_frame(method)
        else:
            self.send_frame(frame.Method(channel_number, method))

    def send_frame(self, frame_value):
        if self.is_closed():
            LOGGER.error("Attempted to send frame when closed")
            raise exceptions.ConnectionClosed

        marshaled_frame = frame_value.marshal()
        self._transport.write(marshaled_frame)
        self._heartbeat.signal_activity()

    def is_closed(self):
        return not self._transport.connected

    def _try_connect(self):
        remaining_connection_attempts = self.parameters.connection_attempts
        while True:
            try:
                self._transport.connect()
                return
            except socket.error:
                if remaining_connection_attempts <= 0:
                    raise
                else:
                    LOGGER.info("Retrying in %i seconds", self.parameters.retry_delay)
                    remaining_connection_attempts -= 1

    def _next_channel_number(self):
        if len(self.channels) >= self.channel_max:
            raise exceptions.NoFreeChannels()

        for num in xrange(1, len(self.channels) + 1):
            if num not in self.channels:
                return num
        return len(self.channels) + 1
