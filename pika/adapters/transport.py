"""Transport implementation."""
# Copyright (C) 2009 Barry Pederson <bp@barryp.org>
#
# This library is free software; you can redistribute it and/or
# modify it under the terms of the GNU Lesser General Public
# License as published by the Free Software Foundation; either
# version 2.1 of the License, or (at your option) any later version.
#
# This library is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
# Lesser General Public License for more details.
#
# You should have received a copy of the GNU Lesser General Public
# License along with this library; if not, write to the Free Software
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301
import errno
import re
import struct
import socket
import ssl

from pika.exceptions import UnexpectedFrameError
from struct import unpack
from contextlib import contextmanager

# Jython does not have this attribute
try:
    from socket import SOL_TCP
except ImportError:  # pragma: no cover
    from socket import IPPROTO_TCP as SOL_TCP  # noqa

try:
    from ssl import SSLError
except ImportError:  # pragma: no cover
    class SSLError(Exception):  # noqa
        """Dummy SSL exception."""

try:
    from os import set_cloexec  # Python 3.4?
except ImportError:  # pragma: no cover
    def set_cloexec(fd, cloexec):  # noqa
        try:
            import fcntl
        except ImportError:  # pragma: no cover
            fcntl = None  # noqa
        """Set flag to close fd after exec."""
        if fcntl is None:
            return
        try:
            FD_CLOEXEC = fcntl.FD_CLOEXEC
        except AttributeError:
            raise NotImplementedError(
                'close-on-exec flag not supported on this platform',
            )
        flags = fcntl.fcntl(fd, fcntl.F_GETFD)
        if cloexec:
            flags |= FD_CLOEXEC
        else:
            flags &= ~FD_CLOEXEC
        return fcntl.fcntl(fd, fcntl.F_SETFD, flags)


def get_errno(exc):
    """Get exception errno (if set).

    Notes:
        :exc:`socket.error` and :exc:`IOError` first got
        the ``.errno`` attribute in Py2.7.
    """
    try:
        return exc.errno
    except AttributeError:
        try:
            # e.args = (errno, reason)
            if isinstance(exc.args, tuple) and len(exc.args) == 2:
                return exc.args[0]
        except AttributeError:
            pass
    return 0


_UNAVAIL = {errno.EAGAIN, errno.EINTR, errno.ENOENT, errno.EWOULDBLOCK}

EMPTY_BUFFER = bytes()

SIGNED_INT_MAX = 0x7FFFFFFF

# Match things like: [fe80::1]:5432, from RFC 2732
IPV6_LITERAL = re.compile(r"\[([\.0-9a-f:]+)\](?::(\d+))?")

# available socket options for TCP level
KNOWN_TCP_OPTS = (
    "TCP_CORK", "TCP_DEFER_ACCEPT", "TCP_KEEPCNT",
    "TCP_KEEPIDLE", "TCP_KEEPINTVL", "TCP_LINGER2",
    "TCP_MAXSEG", "TCP_NODELAY", "TCP_QUICKACK",
    "TCP_SYNCNT", "TCP_WINDOW_CLAMP",
)
TCP_OPTS = [getattr(socket, opt) for opt in KNOWN_TCP_OPTS
            if hasattr(socket, opt)]


class _AbstractTransport(object):
    """Common superclass for TCP and SSL transports."""

    connected = False

    def __init__(self, host, port, connect_timeout=None,
                 read_timeout=None, write_timeout=None,
                 socket_settings=None, raise_on_initial_eintr=True, **kwargs):
        self.connected = True
        self.sock = None
        self.raise_on_initial_eintr = raise_on_initial_eintr
        self._read_buffer = EMPTY_BUFFER
        self.host = host
        self.port = port
        self.connect_timeout = connect_timeout
        self.read_timeout = read_timeout
        self.write_timeout = write_timeout
        self.socket_settings = socket_settings

    def connect(self):
        self._connect(self.host, self.port, self.connect_timeout)
        self._init_socket(
            self.socket_settings, self.read_timeout, self.write_timeout,
        )

    @contextmanager
    def having_timeout(self, timeout):
        if timeout is None:
            yield self.sock
        else:
            sock = self.sock
            prev = sock.gettimeout()
            if prev != timeout:
                sock.settimeout(timeout)
            try:
                yield self.sock
            except SSLError as exc:
                if "timed out" in str(exc):
                    # http://bugs.python.org/issue10272
                    raise socket.timeout()
                elif "The operation did not complete" in str(exc):
                    # Non-blocking SSL sockets can throw SSLError
                    raise socket.timeout()
                raise
            finally:
                if timeout != prev:
                    sock.settimeout(prev)

    def _connect(self, host, port, timeout):
        entries = socket.getaddrinfo(
            host, port, 0, socket.SOCK_STREAM, SOL_TCP,
        )
        for i, res in enumerate(entries):
            af, socktype, proto, canonname, sa = res
            try:
                self.sock = socket.socket(af, socktype, proto)
                try:
                    set_cloexec(self.sock, True)
                except NotImplementedError:
                    pass
                self.sock.settimeout(timeout)
                self.sock.connect(sa)
            except socket.error:
                self.sock.close()
                self.sock = None
                if i + 1 >= len(entries):
                    raise
            else:
                break

    def _init_socket(self, socket_settings, read_timeout, write_timeout):
        try:
            self.sock.settimeout(None)  # set socket back to blocking mode
            self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
            self._set_socket_options(socket_settings)

            # set socket timeouts
            for timeout, interval in ((socket.SO_SNDTIMEO, write_timeout),
                                      (socket.SO_RCVTIMEO, read_timeout)):
                if interval is not None:
                    self.sock.setsockopt(
                        socket.SOL_SOCKET, timeout,
                        struct.pack("ll", interval, 0),
                    )
            self._setup_transport()
        except (OSError, IOError, socket.error) as exc:
            if get_errno(exc) not in _UNAVAIL:
                self.connected = False
            raise

    def _get_tcp_socket_defaults(self, sock):
        return {
            opt: sock.getsockopt(SOL_TCP, opt) for opt in TCP_OPTS
            }

    def _set_socket_options(self, socket_settings):
        if not socket_settings:
            self.sock.setsockopt(SOL_TCP, socket.TCP_NODELAY, 1)
            return

        tcp_opts = self._get_tcp_socket_defaults(self.sock)
        tcp_opts.setdefault(socket.TCP_NODELAY, 1)
        tcp_opts.update(socket_settings)

        for opt, val in tcp_opts.items():
            self.sock.setsockopt(SOL_TCP, opt, val)

    def _read(self, n, initial=False):
        """Read exactly n bytes from the peer."""
        raise NotImplementedError("Must be overriden in subclass")

    def _setup_transport(self):
        """Do any additional initialization of the class."""
        pass

    def _shutdown_transport(self):
        """Do any preliminary work in shutting down the connection."""
        pass

    def _write(self, s):
        """Completely write a string to the peer."""
        raise NotImplementedError("Must be overriden in subclass")

    def close(self):
        if self.sock is not None:
            self._shutdown_transport()
            # Call shutdown first to make sure that pending messages
            # reach the AMQP broker if the program exits after
            # calling this method.
            self.sock.shutdown(socket.SHUT_RDWR)
            self.sock.close()
            self.sock = None
        self.connected = False

    def read_frame(self, unpack=unpack):
        read = self._read
        read_frame_buffer = EMPTY_BUFFER
        try:
            frame_header = read(7, True)
            read_frame_buffer += frame_header
            frame_type, channel, size = unpack('>BHI', frame_header)
            # >I is an unsigned int, but the argument to sock.recv is signed,
            # so we know the size can be at most 2 * SIGNED_INT_MAX
            if size > SIGNED_INT_MAX:
                part1 = read(SIGNED_INT_MAX)
                part2 = read(size - SIGNED_INT_MAX)
                payload = ''.join([part1, part2])
            else:
                payload = read(size)
            read_frame_buffer += payload
            end = read(1)
            ch = ord(end)
            read_frame_buffer += end
        except socket.timeout:
            self._read_buffer = read_frame_buffer + self._read_buffer
            raise
        except (OSError, IOError, SSLError, socket.error) as exc:
            # Don't disconnect for ssl read time outs
            # http://bugs.python.org/issue10272
            if isinstance(exc, SSLError) and 'timed out' in str(exc):
                raise socket.timeout()
            if get_errno(exc) not in _UNAVAIL:
                self.connected = False
            raise
        if ch == 206:  # '\xce'
            return frame_type, channel, payload, read_frame_buffer
        else:
            raise UnexpectedFrameError("Received {0:#04x} while expecting 0xce".format(ch))

    def write(self, s):
        try:
            self._write(s)
        except socket.timeout:
            raise
        except (OSError, IOError, socket.error) as exc:
            if get_errno(exc) not in _UNAVAIL:
                self.connected = False
            raise


class SSLTransport(_AbstractTransport):
    """Transport that works over SSL."""

    def __init__(self, host, port, connect_timeout=None, ssl=None, **kwargs):
        self.sslopts = ssl if isinstance(ssl, dict) else {}
        self._read_buffer = EMPTY_BUFFER
        super(SSLTransport, self).__init__(
            host, port, connect_timeout=connect_timeout, **kwargs)

    def _setup_transport(self):
        """Wrap the socket in an SSL object."""
        self.sock = self._wrap_socket(self.sock, **self.sslopts or {})
        self.sock.do_handshake()
        self._quick_recv = self.sock.read

    def _wrap_socket(self, sock, context=None, **sslopts):
        if context:
            return self._wrap_context(sock, sslopts, **context)
        return ssl.wrap_socket(sock, **sslopts)

    def _wrap_context(self, sock, sslopts, check_hostname=None, **ctx_options):
        ctx = ssl.create_default_context(**ctx_options)
        ctx.check_hostname = check_hostname
        return ctx.wrap_socket(sock, **sslopts)

    def _shutdown_transport(self):
        """Unwrap a Python 2.6 SSL socket, so we can call shutdown()."""
        if self.sock is not None:
            try:
                unwrap = self.sock.unwrap
            except AttributeError:
                return
            self.sock = unwrap()

    def _read(self, n, initial=False,
              _errnos=(errno.ENOENT, errno.EAGAIN, errno.EINTR)):
        # According to SSL_read(3), it can at most return 16kb of data.
        # Thus, we use an internal read buffer like TCPTransport._read
        # to get the exact number of bytes wanted.
        recv = self._quick_recv
        rbuf = self._read_buffer
        try:
            while len(rbuf) < n:
                try:
                    s = recv(n - len(rbuf))  # see note above
                except socket.error as exc:
                    # ssl.sock.read may cause ENOENT if the
                    # operation couldn"t be performed (Issue celery#1414).
                    if exc.errno in _errnos:
                        if initial and self.raise_on_initial_eintr:
                            raise socket.timeout()
                        continue
                    raise
                if not s:
                    raise IOError("Socket closed")
                rbuf += s
        except:
            self._read_buffer = rbuf
            raise
        result, self._read_buffer = rbuf[:n], rbuf[n:]
        return result

    def _write(self, s):
        """Write a string out to the SSL socket fully."""
        write = self.sock.write
        while s:
            try:
                n = write(s)
            except (ValueError, AttributeError):
                # AG: sock._sslobj might become null in the meantime if the
                # remote connection has hung up.
                # In python 3.2, an AttributeError is raised because the SSL
                # module tries to access self._sslobj.write (w/ self._sslobj ==
                # None)
                # In python 3.4, a ValueError is raised is self._sslobj is
                # None. So much for portability... :/
                n = 0
            if not n:
                raise IOError("Socket closed")
            s = s[n:]


class TCPTransport(_AbstractTransport):
    """Transport that deals directly with TCP socket."""

    def _setup_transport(self):
        # Setup to _write() directly to the socket, and
        # do our own buffered reads.
        self._write = self.sock.sendall
        self._read_buffer = EMPTY_BUFFER
        self._quick_recv = self.sock.recv

    def _read(self, n, initial=False, _errnos=(errno.EAGAIN, errno.EINTR)):
        """Read exactly n bytes from the socket."""
        recv = self._quick_recv
        rbuf = self._read_buffer
        try:
            while len(rbuf) < n:
                try:
                    s = recv(n - len(rbuf))
                except socket.error as exc:
                    if exc.errno in _errnos:
                        if initial and self.raise_on_initial_eintr:
                            raise socket.timeout()
                        continue
                    raise
                if not s:
                    raise IOError("Socket closed")
                rbuf += s
        except:
            self._read_buffer = rbuf
            raise

        result, self._read_buffer = rbuf[:n], rbuf[n:]
        return result


def Transport(host, port, connect_timeout=None, ssl=False, **kwargs):
    """Create transport.

    Given a few parameters from the Connection constructor,
    select and create a subclass of _AbstractTransport.
    """
    transport = SSLTransport if ssl else TCPTransport
    return transport(host, port, connect_timeout=connect_timeout, ssl=ssl, **kwargs)
