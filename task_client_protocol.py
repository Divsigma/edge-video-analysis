import asyncio
import time
import functools
import multiprocessing as mp
import json
import random

import buffer
import protocol_buffer
from logging_utils import root_logger

class TaskClientProtocol(asyncio.Protocol):
    def __init__(self, offloader_cbk):
        root_logger.info('initing..')
        super(asyncio.Protocol, self).__init__()
        self.__offloader_cbk = offloader_cbk
        self.__trans = None

        self.__ptb = protocol_buffer.ProtocolBuffer()
        self.__ptb.set_context_handler(functools.partial(TaskClientProtocol.handle_context, self))
        root_logger.info('done init')

    def connection_made(self, transport):
        root_logger.info('connected to {}'.format(transport.get_extra_info('peername')))
        self.__trans = transport

    def data_received(self, data):
        root_logger.debug('receive data (len = {} bytes) on {}'.format(len(data), self.__trans))
        self.__ptb.parse_and_handle_context(data)

    # TODO: handle context
    def handle_context(self, context):
        root_logger.info('>> start handling a context (cmd = {})'.format(context['cmd']))

        if context['cmd'] == 'pulled task':
            prior_task = context['body']
            if prior_task is not None:
                root_logger.info('[OK] got a prior_task (len = {}) to offload, starting offloading'.format(len(prior_task)))
                self.__offloader_cbk(prior_task)
            else:
                root_logger.warning('[OK] got a none prior_task, skipping self.__offloader_cbk')
        elif context['cmd'] == 'failed':
            root_logger.warning('[FAILURE] got a failure context')
        else:
            root_logger.warning('[SKIP] not a pulled prior_task')

        root_logger.info('<< done handling a context (cmd = {})'.format(context['cmd']))

    def write_context(self, context):
        root_logger.info('task client protocol writing context')
        self.__ptb.write_context(self.__trans, context)

if __name__ == '__main__':
    print('{} empty'.format(__name__))

