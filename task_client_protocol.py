import asyncio
import time
import functools
import multiprocessing as mp
import json
import buffer
import random
import protocol_buffer

class TaskClientProtocol(asyncio.Protocol):
    def __init__(self, offloader_cbk):
        print('[{}] initing..'.format(__name__))
        super(asyncio.Protocol, self).__init__()
        self.__offloader_cbk = offloader_cbk
        self.__trans = None

        self.__ptb = protocol_buffer.ProtocolBuffer()
        self.__ptb.set_context_handler(functools.partial(TaskClientProtocol.handle_context, self))

    def connection_made(self, transport):
        print('[{}] task_cli connection made, transport = {}'.format(__name__, transport))
        self.__trans = transport

    def data_received(self, data):
        print('[{}] Got data, len={}'.format(__name__, len(data)))
        self.__ptb.parse_and_handle_context(data)

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

