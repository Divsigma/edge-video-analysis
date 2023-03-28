import asyncio
import time
import functools
import multiprocessing as mp
import json
import buffer
import random

class TaskClientProtocol(asyncio.Protocol):
    def __init__(self, offloader_cbk):
        print('[{}] initing..'.format(__name__))
        super(asyncio.Protocol, self).__init__()
        self.__offloader_cbk = offloader_cbk
        self.__trans = None
        self.__recv_buf = buffer.Buffer()
        self.__context = bytearray(b'')
        self.__context_nbytes = 0
        self.__context_len = -1

    def connection_made(self, transport):
        print('[{}] task_cli connection made, transport = {}'.format(__name__, transport))
        self.__trans = transport

    def data_received(self, data):
        print('[{}] Got data, len={}'.format(__name__, len(data)))
        self.parse_and_handle_context(data)

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
                self.handle_context(self.__context)

                # reset context for next request
                self.__context = bytearray(b'')
                self.__context_nbytes = 0
                self.__context_len = -1

                print('[{}] <<<< done request\n'.format(__name__))

    # TODO: handle context
    def handle_context(self, context):
        print('[{}] cmd={}'.format(__name__, context['cmd']))
        print('[{}] ==== start handling context ====='.format(__name__))

        if context['cmd'] == 'pulled task':
            prior_task = context['body']
            if prior_task is not None:
                print('[{}] [...] Got prior_task(len={})'.format(__name__, len(prior_task)))
                print('[{}] [TRY_OFFLOAD] Got a Task to offload'.format(__name__))
                self.__offloader_cbk(prior_task)
            else:
                print('[{}] [SKIP] Got a None Task, skipping offloader_cbk'.format(__name__))
        elif context['cmd'] == 'failed':
            print('[{}] [TRY_OFFLOAD] Got a Failure'.format(__name__))
        else:
            print('[{}] [SKIP] Not a pulled task'.format(__name__))

        print('[{}] ==== done one handling ====='.format(__name__))

if __name__ == '__main__':
    print('{} empty'.format(__name__))

