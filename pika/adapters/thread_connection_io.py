# coding=utf-8
import uuid
from Queue import Queue

from pika.compat import dict_iteritems


class ListenerHandler(object):
    def __init__(self, acceptable_replies):
        self.queue = Queue(1)
        self.key = str(uuid.uuid1())
        self.acceptable_replies = acceptable_replies

    def process(self, response):
        if self._accept(response):
            self.queue.put(response)
            return True
        return False

    def _accept(self, response_method):
        for expect_reply in self.acceptable_replies:
            arguments = None
            if isinstance(expect_reply, tuple):
                expect_reply, arguments = expect_reply
            if isinstance(response_method, expect_reply) and self._match_instacne_arg(response_method, arguments):
                return response_method
        return None

    def wait(self):
        return self.queue.get()

    def _match_instacne_arg(self, response_method, arguments):
        if arguments is None:
            return True
        for arg_key, arg_val in dict_iteritems(arguments):
            if not hasattr(response_method, arg_key) or getattr(response_method, arg_key) != arg_val:
                return False
        return True


class ThreadConnectionIO(object):
    def __init__(self, connection):
        self.connection = connection
        self.callbacks = dict()

    def rpc0(self, channel_number, method, acceptable_replies=None):
        if not isinstance(acceptable_replies, (type(None), list)):
            raise TypeError("acceptable_replies should be list or None")
        handler = ListenerHandler(acceptable_replies)
        self.add_listener(handler)
        self.connection.send_method(channel_number, method)
        if acceptable_replies is None:
            return
        return handler.wait()

    def handle_frame(self, method_frame):
        for callback in self.callbacks.values():
            if callback.process(method_frame.method):
                del self.callbacks[callback.key]

    def add_listener(self, listener):
        # FIXME thread safe.......囧
        self.callbacks[listener.key] = listener
