# coding=utf-8
import socket

try:
    from unittest import mock
    from unittest.mock import patch
except ImportError:
    import mock
    from mock import patch
try:
    import unittest2 as unittest
except ImportError:
    import unittest


class TestConnectionTests(unittest.TestCase):
    mock_sock_obj = mock.Mock(
        spec_set=socket.socket,
        connect=mock.Mock(side_effect=socket.timeout))
