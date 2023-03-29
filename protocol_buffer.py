import time
import functools
import json
import buffer

class ProtocolBuffer():
    def __init__(self):
        print('[{}] initing..'.format(__name__))

        self.__context_handler = functools.partial(ProtocolBuffer.handle_context, self)
        self.__recv_buf = buffer.Buffer()
        self.__context = bytearray(b'')
        self.__context_nbytes = 0
        self.__context_len = -1
        print('[{}] done init..'.format(__name__))

    def set_context_handler(self, context_handler):
        self.__context_handler = context_handler
        print('[{}] set context_handler to {}'.format(__name__, context_handler))

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
                print('[{}] got request len:{}'.format(__name__, self.__context_len))

            elif self.__context_nbytes < self.__context_len:
                new_context_data = \
                    self.__recv_buf.data[:self.__context_len - self.__context_nbytes]
                self.__context.extend(new_context_data)
                self.__context_nbytes += len(new_context_data)
                self.__recv_buf.retrive(len(new_context_data))
                print('[{}] current context len {}'.format(__name__, self.__context_nbytes))

            # got context
            if self.__context_nbytes == self.__context_len:
                print('[{}] >>>> got request'.format(__name__))
                context_bytes = self.__context.decode()
                # print('[{}] len(context_bytes)={}'.format(__name__, len(context_bytes)))
                assert len(context_bytes) == self.__context_len
                # print('{}'.format(context_bytes))
                # with open('context_bytes', 'w') as f:
                #    print('{}'.format(context_bytes), file=f)
                self.__context = json.loads(context_bytes)

                self.__context_handler(self.__context)

                # reset context for next request
                self.__context = bytearray(b'')
                self.__context_nbytes = 0
                self.__context_len = -1

                print('[{}] <<<< done request\n'.format(__name__))

    # TODO: default context handler
    def handle_context(self, context):
        print('[{}] cmd={}'.format(__name__, context['cmd']))
        print('[{}] ==== start handling context ====='.format(__name__))
        print('[{}] default context_handler do NOTHING'.format(__name__))

if __name__ == '__main__':
    print('{} empty'.format(__name__))

