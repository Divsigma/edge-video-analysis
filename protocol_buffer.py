import time
import functools
import json
import buffer

from logging_utils import root_logger

class ProtocolBuffer():
    def __init__(self):
        root_logger.info('initing...')
        # print('[{}] initing..'.format(__name__))

        self.__context_handler = functools.partial(ProtocolBuffer.handle_context, self)
        self.__recv_buf = buffer.Buffer()
        self.__context = bytearray(b'')
        self.__context_nbytes = 0
        self.__context_len = -1
        root_logger.info('done init..')
        # print('[{}] done init..'.format(__name__))

    def set_context_handler(self, context_handler):
        self.__context_handler = context_handler
        root_logger.info('set context_handler to {}'.format(context_handler))
        # print('[{}] set context_handler to {}'.format(__name__, context_handler))

    # TODO: parse and handle context
    # stream: <len>(4 bytes)<body>('len' bytes)<len><body>...
    # encode-decode: json
    def parse_and_handle_context(self, data):
        self.__recv_buf.append(data)
        while self.__recv_buf.readable_bytes() >= 4:
            # parse context
            if self.__context_len == -1:
                assert self.__recv_buf.readable_bytes() >= 4
                len_data = self.__recv_buf.retrive_as_bytes(4)
                self.__context_len = int.from_bytes(
                    len_data, byteorder='big', signed=False
                )
                root_logger.info('- start receiving a request (expected len: {} bytes)'.format(self.__context_len))
                # print('[{}] got request len:{}'.format(__name__, self.__context_len))

            elif self.__context_nbytes < self.__context_len:
                new_context_data = \
                    self.__recv_buf.data[:self.__context_len - self.__context_nbytes]
                self.__context.extend(new_context_data)
                self.__context_nbytes += len(new_context_data)
                self.__recv_buf.retrive(len(new_context_data))
                # print('[{}] current context len {}'.format(__name__, self.__context_nbytes))

            # got context
            if self.__context_nbytes == self.__context_len:
                root_logger.info('> start handling a request (actual len: {} bytes)'.format(self.__context_nbytes))
                # print('[{}] >>>> got request'.format(__name__))

                context_bytes = self.__context.decode()
                assert len(context_bytes) == self.__context_len
                # print('{}'.format(context_bytes))
                # with open('context_bytes', 'w') as f:
                #    print('{}'.format(context_bytes), file=f)
                self.__context = json.loads(context_bytes)

                self.__context_handler(self.__context)

                root_logger.info('< done handling a request (len: {} bytes)'.format(self.__context_len))

                # reset context for next request
                self.__context = bytearray(b'')
                self.__context_nbytes = 0
                self.__context_len = -1

                # print('[{}] <<<< done request\n'.format(__name__))

    def write_context(self, trans, context):
        json_context = json.dumps(context)
        length_json_context = len(json_context)

        # trans is a object of asyncio.Transport
        trans.write(length_json_context.to_bytes(length=4, byteorder='big', signed=False))
        trans.write(json_context.encode())

    # TODO: default context handler
    def handle_context(self, context):
        root_logger.warning('>> start handling a context (cmd={})'.format(context['cmd']))
        root_logger.warning('default context_handler do NOTHING')
        root_logger.warning('<< done handling a context (cmd={})'.format(context['cmd']))
        # print('[{}] cmd={}'.format(__name__, context['cmd']))
        # print('[{}] ==== start handling context ====='.format(__name__))
        # print('[{}] default context_handler do NOTHING'.format(__name__))

if __name__ == '__main__':
    print('{} empty'.format(__name__))

