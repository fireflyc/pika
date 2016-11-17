# coding=utf-8
from Queue import Queue

from pika import exceptions
from pika.compat import dict_iteritems


class ThreadConnectionIO(object):
    def __init__(self, connection):
        self.connection = connection
        self.queue = Queue()

    def rpc0(self, channel_number, method, acceptable_replies=None):
        if not isinstance(acceptable_replies, (type(None), list)):
            raise TypeError("acceptable_replies should be list or None")
        self.connection.send_method(channel_number, method)
        if not acceptable_replies:
            return
        response = self.queue.get()
        for expect_reply in acceptable_replies:
            arguments = None
            if isinstance(expect_reply, tuple):
                expect_reply, arguments = expect_reply
            if isinstance(response, expect_reply) and self._match_instacne_arg(response, arguments):
                return response
        raise exceptions.UnexpectedFrameError(response)

    def _match_instacne_arg(self, response, arguments):
        if arguments is None:
            return True
        for arg_key, arg_val in dict_iteritems(arguments):
            if not hasattr(response, arg_key) or getattr(response, arg_key) != arg_val:
                return False
        return True

    def handle_frame(self, method_frame):
        self.queue.put(method_frame.method)
