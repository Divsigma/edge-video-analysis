import asyncio
import functools

import protocol_buffer
from logging_utils import root_logger

class CloudServerProtocol(asyncio.Protocol):
    def __init__(self, offloader_cbk):
        root_logger.info('initing...')
        super(asyncio.Protocol, self).__init__()

        self.__trans = None
        self.__offloader_cbk = offloader_cbk

        self.__ptb = protocol_buffer.ProtocolBuffer()
        self.__ptb.set_context_handler(functools.partial(CloudServerProtocol.handle_context, self))

        root_logger.info('done init...')

    def connection_made(self, transport):
        root_logger.info('connection from {}'.format(transport.get_extra_info('peername')))
        self.__trans = transport

    def eof_received(self):
        root_logger.info('closing {}'.format(self.__trans))
        self.__trans.write('server: done writing'.encode())
        self.__trans.close()

    def data_received(self, data):
        root_logger.debug('data received (len = {}) on {}'.format(len(data), self.__trans))
        self.__ptb.parse_and_handle_context(data)

    def handle_context(self, context):
        root_logger.info('>> start handling a context (cmd = {})'.format(context['cmd']))

        if context['cmd'] == 'task':
            # TODO
            root_logger.info('[OK] receive prior_task from edge, try offloading')
            prior_task = context['body']
            if self.__offloader_cbk:
                self.__offloader_cbk(prior_task)
            else:
                root_logger.warning('self.__offloader_cbk is None')
        else:
            root_logger.warning('[SKIP] not support cmd')

        root_logger.info('<< done handling a context (cmd = {})'.format(context['cmd']))

async def test_main():
    # Get a reference to the event loop as we plan to use
    # low-level APIs.
    loop = asyncio.get_running_loop()

    server = await loop.create_server(
        lambda: CloudServerProtocol(offloader_cbk=None),
        '0.0.0.0', 9999
    )

    root_logger.info('create server on port = 9999')

    while True:
        await asyncio.sleep(5)
        root_logger.info('wake up once')

    # async with server:
    #    await server.serve_forever()


if __name__ == '__main__':
    asyncio.run(test_main())
