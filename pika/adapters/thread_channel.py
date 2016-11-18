# coding=utf-8
import logging
import uuid
import thread

import math
from Queue import Queue

from pika import spec, frame, exceptions
from pika.adapters.thread_connection_io import ThreadConnectionIO
from pika.compat import unicode_type, is_integer, dictkeys

LOGGER = logging.getLogger(__name__)


class ThreadConnectionChannel(ThreadConnectionIO):
    def __init__(self, connection, channel_number):
        super(ThreadConnectionChannel, self).__init__(connection)
        self.connection = connection
        self.channel_number = channel_number
        self._content_assembler = ContentFrameAssembler()
        self.is_closed = False
        self._consumers = dict()
        self._consumers_with_noack = set()
        self._delivery_confirmation = False
        self._puback_return = None
        self._puback_queue = Queue()

    def open(self):
        self.rpc(spec.Channel.Open(), [spec.Channel.OpenOk])

    def basic_ack(self, delivery_tag=0, multiple=False):
        self._validate_connection_and_channel()
        return self.send_method(spec.Basic.Ack(delivery_tag, multiple))

    def basic_cancel(self, consumer_tag='', nowait=False):
        self._validate_connection_and_channel()
        if consumer_tag not in self._consumers:
            LOGGER.warning('basic_cancel - consumer not found: %s', consumer_tag)
            return

        LOGGER.debug('Cancelling consumer: %s (nowait=%s)',
                     consumer_tag, nowait)
        del self._consumers[consumer_tag]
        return self.rpc(spec.Basic.Cancel(consumer_tag=consumer_tag, nowait=nowait),
                        [(spec.Basic.CancelOk, {'consumer_tag': consumer_tag})] if nowait is False else [])

    def basic_consume(self, consumer_callback,
                      queue='',
                      no_ack=False,
                      exclusive=False,
                      consumer_tag=None,
                      arguments=None):
        self._validate_connection_and_channel()
        if not consumer_tag:
            consumer_tag = self._generate_consumer_tag()

        if consumer_tag in self._consumers:
            raise exceptions.DuplicateConsumerTag(consumer_tag)

        if no_ack:
            self._consumers_with_noack.add(consumer_tag)

        self._consumers[consumer_tag] = consumer_callback
        self.rpc(spec.Basic.Consume(queue=queue,
                                    consumer_tag=consumer_tag,
                                    no_ack=no_ack,
                                    exclusive=exclusive,
                                    arguments=arguments or dict()),
                 [(spec.Basic.ConsumeOk, {'consumer_tag': consumer_tag})])

        return consumer_tag

    def _generate_consumer_tag(self):
        return 'ctag%i.%s' % (self.channel_number,
                              uuid.uuid4().hex)

    def basic_get(self, queue='', no_ack=False):
        self._validate_connection_and_channel()
        get = self.rpc(spec.Basic.Get(queue=queue, no_ack=no_ack), [spec.Basic.GetOk, spec.Basic.GetEmpty])
        if isinstance(spec.Basic.GetEmpty, get):
            return None
        else:
            return get

    def basic_nack(self, delivery_tag=None, multiple=False, requeue=True):
        self._validate_connection_and_channel()
        return self.send_method(spec.Basic.Nack(delivery_tag, multiple,
                                                requeue))

    def publish(self, exchange, routing_key, body,
                properties=None, mandatory=False, immediate=False):
        if self._delivery_confirmation:
            # In publisher-acknowledgments mode
            self.basic_publish(exchange=exchange,
                               routing_key=routing_key,
                               body=body,
                               properties=properties,
                               mandatory=mandatory,
                               immediate=immediate)
            conf_method = self._puback_queue.get()
            if isinstance(conf_method, spec.Basic.Nack):
                # Broker was unable to process the message due to internal
                # error
                LOGGER.warn(
                    "Message was Nack'ed by broker: nack=%r; channel=%s; "
                    "exchange=%s; routing_key=%s; mandatory=%r; "
                    "immediate=%r", conf_method, self.channel_number,
                    exchange, routing_key, mandatory, immediate)
                if self._puback_return is not None:
                    returned_messages = [self._puback_return]
                    self._puback_return = None
                else:
                    returned_messages = []
                raise exceptions.NackError(returned_messages)
            else:
                assert isinstance(conf_method, spec.Basic.Ack), (conf_method)

                if self._puback_return is not None:
                    # Unroutable message was returned
                    messages = [self._puback_return]
                    self._puback_return = None
                    raise exceptions.UnroutableError(messages)
        else:
            # In non-publisher-acknowledgments mode
            self.basic_publish(exchange=exchange,
                               routing_key=routing_key,
                               body=body,
                               properties=properties,
                               mandatory=mandatory,
                               immediate=immediate)

    def basic_publish(self, exchange, routing_key, body,
                      properties=None,
                      mandatory=False,
                      immediate=False):
        self._validate_connection_and_channel()
        if immediate:
            LOGGER.warning('The immediate flag is deprecated in RabbitMQ')
        if isinstance(body, unicode_type):
            body = body.encode('utf-8')
        properties = properties or spec.BasicProperties()
        self.send_method(spec.Basic.Publish(exchange=exchange,
                                            routing_key=routing_key,
                                            mandatory=mandatory,
                                            immediate=immediate), (properties, body))

    def basic_qos(self,
                  prefetch_size=0,
                  prefetch_count=0,
                  all_channels=False):
        self._validate_connection_and_channel()
        return self.rpc(spec.Basic.Qos(prefetch_size, prefetch_count,
                                       all_channels),
                        [spec.Basic.QosOk])

    def basic_reject(self, delivery_tag, requeue=True):
        self._validate_connection_and_channel()
        if not is_integer(delivery_tag):
            raise TypeError('delivery_tag must be an integer')
        return self.send_method(spec.Basic.Reject(delivery_tag, requeue))

    def basic_recover(self, requeue=False):
        self._validate_connection_and_channel()
        return self.rpc(spec.Basic.Recover(requeue), [spec.Basic.RecoverOk])

    def close(self, reply_code=0, reply_text="Normal Shutdown"):
        if self.is_closed:
            raise exceptions.ChannelClosed('Already closed: %s' % self)

        LOGGER.info('Closing channel (%s): %r on %s',
                    reply_code, reply_text, self)

        for consumer_tag in dictkeys(self._consumers):
            self.basic_cancel(consumer_tag=consumer_tag)
        self.is_closed = True
        return self.rpc(spec.Channel.Close(reply_code, reply_text, 0, 0), [spec.Channel.CloseOk])

    def confirm_delivery(self, nowait=False):
        self._validate_connection_and_channel()

        if not (self.connection.publisher_confirms and self.connection.basic_nack):
            raise exceptions.MethodNotImplemented('Not Supported on Server')
        self._delivery_confirmation = True
        return self.rpc(spec.Confirm.Select(nowait), [spec.Confirm.SelectOk] if nowait is False else [])

    @property
    def consumer_tags(self):
        return dictkeys(self._consumers)

    def exchange_bind(self,
                      destination=None,
                      source=None,
                      routing_key='',
                      nowait=False,
                      arguments=None):
        self._validate_connection_and_channel()
        return self.rpc(spec.Exchange.Bind(0, destination, source, routing_key,
                                           nowait, arguments or dict()),
                        [spec.Exchange.BindOk] if nowait is False
                        else [])

    def exchange_declare(self,
                         exchange=None,
                         exchange_type='direct',
                         passive=False,
                         durable=False,
                         auto_delete=False,
                         internal=False,
                         nowait=False,
                         arguments=None):
        self._validate_connection_and_channel()

        return self.rpc(spec.Exchange.Declare(0, exchange, exchange_type,
                                              passive, durable, auto_delete,
                                              internal, nowait,
                                              arguments or dict()),
                        [spec.Exchange.DeclareOk] if nowait is False else [])

    def exchange_delete(self,
                        exchange=None,
                        if_unused=False,
                        nowait=False):
        self._validate_connection_and_channel()
        return self.rpc(spec.Exchange.Delete(0, exchange, if_unused, nowait),
                        [spec.Exchange.DeleteOk] if nowait is False else [])

    def exchange_unbind(self,
                        destination=None,
                        source=None,
                        routing_key='',
                        nowait=False,
                        arguments=None):
        self._validate_connection_and_channel()
        return self.rpc(spec.Exchange.Unbind(0, destination, source,
                                             routing_key, nowait, arguments),
                        [spec.Exchange.UnbindOk] if nowait is False else [])

    def flow(self, active):
        self._validate_connection_and_channel()
        return self.rpc(spec.Channel.Flow(active), [spec.Channel.FlowOk])

    def queue_bind(self, queue, exchange,
                   routing_key=None,
                   nowait=False,
                   arguments=None):
        self._validate_connection_and_channel()
        replies = [spec.Queue.BindOk] if nowait is False else []
        if routing_key is None:
            routing_key = queue
        return self.rpc(spec.Queue.Bind(0, queue, exchange, routing_key,
                                        nowait, arguments or dict()), replies)

    def queue_declare(self,
                      queue='',
                      passive=False,
                      durable=False,
                      exclusive=False,
                      auto_delete=False,
                      nowait=False,
                      arguments=None):
        if queue:
            condition = (spec.Queue.DeclareOk,
                         {'queue': queue})
        else:
            condition = spec.Queue.DeclareOk
        replies = [condition] if nowait is False else []
        self._validate_connection_and_channel()
        return self.rpc(spec.Queue.Declare(0, queue, passive, durable,
                                           exclusive, auto_delete, nowait,
                                           arguments or dict()), replies)

    def queue_delete(self,
                     queue='',
                     if_unused=False,
                     if_empty=False,
                     nowait=False):
        replies = [spec.Queue.DeleteOk] if nowait is False else []
        self._validate_connection_and_channel()
        return self.rpc(spec.Queue.Delete(0, queue, if_unused, if_empty,
                                          nowait), replies)

    def queue_purge(self, queue='', nowait=False):
        replies = [spec.Queue.PurgeOk] if nowait is False else []
        self._validate_connection_and_channel()
        return self.rpc(spec.Queue.Purge(0, queue, nowait), replies)

    def queue_unbind(self,
                     queue='',
                     exchange=None,
                     routing_key=None,
                     arguments=None):
        self._validate_connection_and_channel()
        if routing_key is None:
            routing_key = queue
        return self.rpc(spec.Queue.Unbind(0, queue, exchange, routing_key,
                                          arguments or dict()), [spec.Queue.UnbindOk])

    def tx_commit(self):
        self._validate_connection_and_channel()
        return self.rpc(spec.Tx.Commit(), [spec.Tx.CommitOk])

    def tx_rollback(self):
        self._validate_connection_and_channel()
        return self.rpc(spec.Tx.Rollback(), [spec.Tx.RollbackOk])

    def tx_select(self):
        self._validate_connection_and_channel()
        return self.rpc(spec.Tx.Select(), [spec.Tx.SelectOk])

    def handle_pub_ack(self, frame_value):
        self._puback_queue.put(frame_value.method)

    def handle_frame(self, frame_value):
        finish, content = self._content_assembler.process(frame_value)
        if not finish:
            return
        if not isinstance(content, tuple):
            ThreadConnectionIO.handle_frame(self, frame_value)
            return
        (method_frame, header_frame, body) = content
        if isinstance(method_frame, spec.Basic.GetOk):
            ThreadConnectionIO.handle_frame(self, frame_value)
            return

        # TODO add consumer thread
        thread.start_new_thread(self.deliver_to_consumer, content)

    def deliver_to_consumer(self, method_frame, header_frame, body):
        if isinstance(method_frame.method, spec.Basic.Deliver):
            self._on_deliver(method_frame, header_frame, body)
        elif isinstance(method_frame.method, spec.Basic.Return):
            self._on_return(method_frame, header_frame, body)

    def _on_deliver(self, method_frame, header_frame, body):
        consumer_tag = method_frame.method.consumer_tag

        if consumer_tag not in self._consumers:
            LOGGER.warn('Unexpected delivery: %r', method_frame)
            self.basic_reject(method_frame.method.delivery_tag)
            return

        self._consumers[consumer_tag](self, method_frame.method, header_frame.properties, body)

    def _on_return(self, method_frame, header_frame, body):
        assert isinstance(method_frame, spec.Basic.Return), method_frame
        assert isinstance(method_frame.properties, spec.BasicProperties), method_frame.properties
        LOGGER.warn(
            "Published message was returned: _delivery_confirmation=%s; "
            "channel=%s; method=%r; properties=%r; body_size=%d; "
            "body_prefix=%.255r", self._delivery_confirmation,
            self.channel_number, method_frame, method_frame.properties,
            len(body) if body is not None else None, body)
        self._puback_return = (method_frame, header_frame, body)

    def _validate_connection_and_channel(self):
        if self.connection.is_closed():
            raise exceptions.ConnectionClosed()
        if self.is_closed:
            raise exceptions.ChannelClosed()

    def send_method(self, method, content=None):
        if content is None:
            self.connection.send_method(self.channel_number, method)
        else:
            self._send_message(self.channel_number, method, content)

    def _send_message(self, channel_number, method, content=None):
        length = len(content[1])
        self.connection.send_frame(frame.Method(channel_number, method).marshal())
        self.connection.send_frame(frame.Header(channel_number, length, content[0]))
        if content[1]:
            body_max_length = (self.connection.frame_max - spec.FRAME_HEADER_SIZE - spec.FRAME_END_SIZE)
            chunks = int(math.ceil(float(length) / body_max_length))
            for chunk in xrange(0, chunks):
                start = chunk * body_max_length
                end = start + body_max_length
                if end > length:
                    end = length
                self.connection.send_frame(frame.Body(channel_number, content[1][start:end]))

    def rpc(self, method, acceptable_replies=None):
        return self.rpc0(self.channel_number, method, acceptable_replies)


class ContentFrameAssembler(object):
    """Handle content related frames, building a message and return the message
    back in three parts upon receipt.

    """

    def __init__(self):
        """Create a new instance of the conent frame assembler.

        """
        self._method_frame = None
        self._header_frame = None
        self._seen_so_far = 0
        self._body_fragments = list()

    def process(self, frame_value):
        if isinstance(frame_value, frame.Method):
            if spec.has_content(frame_value.method.INDEX):
                self._method_frame = frame_value
                return False, None
            return True, frame_value
        elif isinstance(frame_value, frame.Header):
            self._header_frame = frame_value
            if frame_value.body_size == 0:
                return self._finish()
            return False, None
        elif isinstance(frame_value, frame.Body):
            return self._handle_body_frame(frame_value)
        else:
            raise exceptions.UnexpectedFrameError(frame_value)

    def _finish(self):
        """Invoked when all of the message has been received

        :rtype: tuple(pika.frame.Method, pika.frame.Header, str)

        """
        content = (self._method_frame, self._header_frame,
                   b''.join(self._body_fragments))
        self._reset()
        return True, content

    def _handle_body_frame(self, body_frame):
        """Receive body frames and append them to the stack. When the body size
        matches, call the finish method.

        :param Body body_frame: The body frame
        :raises: pika.exceptions.BodyTooLongError
        :rtype: tuple(pika.frame.Method, pika.frame.Header, str)|None

        """
        self._seen_so_far += len(body_frame.fragment)
        self._body_fragments.append(body_frame.fragment)
        if self._seen_so_far == self._header_frame.body_size:
            return self._finish()
        elif self._seen_so_far > self._header_frame.body_size:
            raise exceptions.BodyTooLongError(self._seen_so_far,
                                              self._header_frame.body_size)
        return False, None

    def _reset(self):
        """Reset the values for processing frames"""
        self._method_frame = None
        self._header_frame = None
        self._seen_so_far = 0
        self._body_fragments = list()
