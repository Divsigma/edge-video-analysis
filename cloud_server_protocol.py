import asyncio
import functools

import protocol_buffer

class CloudServerProtocol(asyncio.Protocol):
    def __init__(self, offloader_cbk):
        print('[{}] initing...'.format(__name__))
        super(asyncio.Protocol, self).__init__()

        self.__transport = None
        self.__offloader_cbk = offloader_cbk

        self.__ptb = protocol_buffer.ProtocolBuffer()
        self.__ptb.set_context_handler(functools.partial(CloudServerProtocol.handle_context, self))

        print('[{}] done init...'.format(__name__))

    def connection_made(self, transport):
        peername = transport.get_extra_info('peername')
        print('Connection from {}'.format(peername))
        self.__transport = transport

    def eof_received(self):
        print('Closing: {}'.format(self.__transport))
        self.__transport.write('server: done writing'.encode())
        self.__transport.close()

    def data_received(self, data):
        print('Data received (len={}) on {}'.format(len(data), self.__transport))
        self.__ptb.parse_and_handle_context(data)

    def handle_context(self, context):
        print('[{}] cmd = {}'.format(__name__, context['cmd']))
        print('[{}] ==== start handling context ===='.format(__name__))

        if context['cmd'] == 'task':
            # TODO
            print('[{}] receive prior task from edge'.format(__name__))
            prior_task = context['body']
            if self.__offloader_cbk:
                self.__offloader_cbk(prior_task)
            else:
                print('[{}] self.__offloader_cbk is None'.format(__name__))
        else:
            print('[{}] [SKIP] not support cmd'.format(__name__))

        print('[{}] ==== done one handling ===='.format(__name__))

async def test_main():
    # Get a reference to the event loop as we plan to use
    # low-level APIs.
    loop = asyncio.get_running_loop()

    server = await loop.create_server(
        lambda: CloudServerProtocol(offloader_cbk=None),
        '0.0.0.0', 9999
    )

    print('[{}] create server: {}'.format(__name__, server))

    while True:
        await asyncio.sleep(5)
        print('[{}] wake up once'.format(__name__))

    # async with server:
    #    await server.serve_forever()


if __name__ == '__main__':
    asyncio.run(test_main())
