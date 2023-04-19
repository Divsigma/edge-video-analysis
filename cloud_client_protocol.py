import asyncio
import functools
import json

import protocol_buffer

from logging_utils import root_logger

class CloudClientProtocol(asyncio.Protocol):
    def __init__(self):
        print('[{}] initing...'.format(__name__))
        super(asyncio.Protocol, self).__init__()

        self.__trans = None
        self.__ptb = protocol_buffer.ProtocolBuffer()
        self.__ptb.set_context_handler(functools.partial(CloudClientProtocol.handle_context, self))

        print('[{}] inited...'.format(__name__))

    def connection_made(self, transport):
        print('upload connection made, transport = {}'.format(transport))
        self.__trans = transport

    def data_received(self, data):
        print('Got data: {}'.format(data))
        self.__ptb.parse_and_handle_context(data)

    def handle_context(self, context):
        print('[{}] cmd = {}'.format(__name__, context['cmd']))
        print('[{}] ==== start handling context ===='.format(__name__))

        if context['cmd'] == 'strategy':
            # TODO
            print('[{}] receive a strategy from cloud'.format(__name__))
        else:
            print('[{}] [SKIP] not a strategy'.format(__name__))

        print('[{}] ==== done one handling ===='.format(__name__))

    def write_context(self, context):
        root_logger.info('cloud client protocol writing context')
        self.__ptb.write_context(self.__trans, context)

async def client_test_main():
    loop = asyncio.get_running_loop()

    trans, protocol = await loop.create_connection(
        lambda: CloudClientProtocol(),
        '127.0.0.1',
        9999
    )

    while True:

        await asyncio.sleep(1)
        print('[{}] write one request'.format(__name__))

        request = dict()
        request['cmd'] = 'task'
        request['body'] = None

        json_req = json.dumps(request)
        len_json_req = len(json_req)

        trans.write(len_json_req.to_bytes(length=4, byteorder='big', signed=False))
        trans.write(json_req.encode())

if __name__ == '__main__':
    print('cloudclientprotocol main')
    asyncio.run(client_test_main())
