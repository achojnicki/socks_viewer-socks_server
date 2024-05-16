from adistools.adisconfig import adisconfig
from adistools.log import Log
from socketserver import ThreadingMixIn, TCPServer, StreamRequestHandler
from json import dumps
from threading import Thread
from uuid import uuid4
from redis import StrictRedis

import select
import socket
import struct
import functools


class Server(ThreadingMixIn, TCPServer):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self._config=adisconfig('/opt/adistools/configs/socks_viewer-socks_server.yaml')
        self._log=Log(
            parent=self,
            rabbitmq_host=self._config.rabbitmq.host,
            rabbitmq_port=self._config.rabbitmq.port,
            rabbitmq_user=self._config.rabbitmq.user,
            rabbitmq_passwd=self._config.rabbitmq.password,
            debug=self._config.log.debug,
            )

        self._redis_cli=StrictRedis(
            host=self._config.redis.host,
            port=self._config.redis.port,
            db=self._config.redis.db
        )


class SocksProxy(StreamRequestHandler):
    name="socks_viewer-socks_server"
    SOCKS_VERSION = 5

    CONNECTION_ACCEPTED=1
    CONNECTION_OPEN=2
    CONNECTION_CLOSED=3
    CONNECTION_DATA_TRANSMITING=4

    CONNECTION_STATUSES={
        CONNECTION_ACCEPTED: "CONNECTION_ACCEPTED",
        CONNECTION_OPEN: "CONNECTION_OPEN",
        CONNECTION_CLOSED: "CONNECTION_CLOSED",
        CONNECTION_DATA_TRANSMITING: "CONNECTION_DATA_TRANSMITING"
    }



    def _report_connection(self,connection_status, connection_uuid, local_addr=None, local_port=None, remote_addr=None, remote_addr_domain=None, remote_port=None):
        data={
            "connection_uuid": connection_uuid,
            "connection_status": self.CONNECTION_STATUSES[connection_status],
        }

        if local_addr:
            data['local_addr']=local_addr if type(local_addr) is str else local_addr.decode('utf-8')
        if local_port:
            data['local_port']=local_port

        if remote_addr:
            data['remote_addr']=remote_addr if type(remote_addr) is str else remote_addr.decode('utf-8')
        if remote_addr_domain:
            data['remote_addr_domain']=remote_addr_domain if type(remote_addr_domain) is str else remote_addr_domain.decode('utf-8')
        if remote_port:
            data['remote_port']=remote_port

        print(data)

        self.server._redis_cli.publish('socks_viewer-connections', dumps(data))
        






    def handle(self):
        connection_uuid=str(uuid4())
        self._report_connection(self.CONNECTION_ACCEPTED, connection_uuid, self.client_address[0], self.client_address[1])

        # greeting header
        # read and unpack 2 bytes from a client
        header = self.connection.recv(2)
        version, nmethods = struct.unpack("!BB", header)

        # socks 5
        assert version == self.SOCKS_VERSION
        assert nmethods > 0

        # get available methods
        methods = self.get_available_methods(nmethods)

        # send welcome message
        self.connection.sendall(struct.pack("!BB", self.SOCKS_VERSION, 0))

        # request
        version, cmd, _, address_type = struct.unpack("!BBBB", self.connection.recv(4))
        assert version == self.SOCKS_VERSION

        if address_type == 1:  # IPv4
            address = socket.inet_ntoa(self.connection.recv(4))
            address_domain=None
        
        elif address_type == 3:  # Domain name
            domain_length = self.connection.recv(1)[0]
            address_domain = self.connection.recv(domain_length)
            address = socket.gethostbyname(address_domain)
        port = struct.unpack('!H', self.connection.recv(2))[0]

        # reply
        try:
            if cmd == 1:  # CONNECT
                remote = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                remote.connect((address, port))
                bind_address = remote.getsockname()
                self._report_connection(self.CONNECTION_OPEN, connection_uuid, self.client_address[0], self.client_address[1], address, address_domain, port)
            else:
                self.server.close_request(self.request)
                self._report_connection(self.CONNECTION_CLOSED, connection_uuid)

            addr = struct.unpack("!I", socket.inet_aton(bind_address[0]))[0]
            port = bind_address[1]
            reply = struct.pack("!BBBBIH", self.SOCKS_VERSION, 0, 0, 1,
                                addr, port)

        except Exception as err:
            raise
            # return connection refused error
            reply = self.generate_failed_reply(address_type, 5)

        self.connection.sendall(reply)

        # establish data exchange
        if reply[1] == 0 and cmd == 1:
            self.exchange_loop(self.connection, remote, connection_uuid, self)

        self._report_connection(self.CONNECTION_CLOSED, connection_uuid)
        self.server.close_request(self.request)

    def get_available_methods(self, n):
        methods = []
        for i in range(n):
            methods.append(ord(self.connection.recv(1)))
        return methods

    def generate_failed_reply(self, address_type, error_number):
        return struct.pack("!BBBBIH", self.SOCKS_VERSION, error_number, 0, address_type, 0, 0)

    def exchange_loop(self, client, remote, connection_uuid, socks):

        while True:

            # wait until client or remote is available for read
            r, w, e = select.select([client, remote], [], [])

            if r:
                self._report_connection(self.CONNECTION_DATA_TRANSMITING, connection_uuid )

            if client in r:
                data = client.recv(4096)
                if remote.send(data) <= 0:
                    break

            if remote in r:
                data = remote.recv(4096)
                if client.send(data) <= 0:
                    break


if __name__ == '__main__':
    with Server(('127.0.0.1', 8080), SocksProxy) as server:
        server.serve_forever()