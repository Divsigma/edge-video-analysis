import asyncio
import queue
import buffer
import json
import time
import functools
import argparse

import protocol_buffer
from logging_utils import root_logger

class TaskQueue:
    def __init__(self):
        self.__q = queue.PriorityQueue(-1)

    def put(self, prior, prior_task):
        root_logger.info('to put a prior_task (prior = {}), qsize = {}'.format(prior, self.__q.qsize()))
        try:
            self.__q.put_nowait((prior, prior_task))
            return True
        except queue.Full:
            root_logger.warning('queue is full, qsize = {}, returning False'.format(self.__q.qsize()))
            return False

    def get(self):
        root_logger.info('to get a prior_task, qsize = {}'.format(self.__q.qsize()))
        try:
            prior, prior_task = self.__q.get_nowait()
            return (prior, prior_task)
        except queue.Empty:
            root_logger.warning('queue is empty, returning None')
            return (None, None)

    def qsize(self):
        return self.__q.qsize()

class TaskServerProtocol(asyncio.Protocol):
    def __init__(self, task_q):
        root_logger.info('initing...')
        super(asyncio.Protocol, self).__init__()

        self.__task_q = task_q

        self.__ptb = protocol_buffer.ProtocolBuffer()
        self.__ptb.set_context_handler(functools.partial(TaskServerProtocol.handle_context, self))

        root_logger.info('done init')

    def connection_made(self, transport):
        root_logger.info('connection from {}'.format(transport.get_extra_info('peername')))
        self.__trans = transport

    def eof_received(self):
        root_logger.info('closing {}'.format(self.__trans))
        self.__trans.write('server: done writing'.encode())
        self.__trans.close()

    def data_received(self, data):
        root_logger.debug('receive data (len = {} bytes) on {}'.format(len(data), self.__trans))
        self.__ptb.parse_and_handle_context(data)

    # TODO: handle context
    def handle_context(self, context):
        root_logger.info('>> start handling a context (cmd = {})'.format(context['cmd']))

        if context['cmd'] == 'pull':
            root_logger.info('try to PULL prior_task from self.__task_q')
            prior, prior_task = self.__task_q.get()
            if prior_task:
                root_logger.info('get prior_task from self.__task_q: prior_task(len={})(prior={})'.format(len(prior_task), prior))

            resp = dict()
            resp['cmd'] = 'pulled task'
            resp['body'] = prior_task
            json_resp = json.dumps(resp)
            len_json_resp = len(json_resp)

            self.__trans.write(len_json_resp.to_bytes(length=4, byteorder='big', signed=False))
            self.__trans.write(json_resp.encode())

        if context['cmd'] == 'push':
            prior_task = context['body']
            root_logger.info('try to PUSH prior_task(prior={}) to self.__task_q'.format(prior_task['prior']))
            ret = self.__task_q.put(prior_task['prior'], prior_task)
            if ret == False:
                root_logger.warning('push failure')
                resp = dict()
                resp['cmd'] = 'failed'
                json_resp = json.dumps(resp)
                len_json_resp = len(json_resp)

                self.__trans.write(len_json_resp.to_bytes(length=4, byteorder='big', signed=False))
                self.__trans.write(json_resp.encode())

            # demo: echo task
            # json_ctx = json.dumps(context)
            # len_json_ctx = len(json_ctx)
            # self.transport.write(len_json_ctx.to_bytes(length=4, byteorder='big', signed=False))
            # self.transport.write(json_ctx.encode())

        root_logger.info('<< done handling a context (cmd = {})'.format(context['cmd']))


async def task_q_server_loop(serv_port):
    # Get a reference to the event loop as we plan to use
    # low-level APIs.
    loop = asyncio.get_running_loop()

    global_task_q = TaskQueue()

    server = await loop.create_server(
        lambda: TaskServerProtocol(global_task_q),
        '127.0.0.1', serv_port)

    root_logger.info('start awaiting on server(port = {})...'.format(serv_port))

    async with server:
        await server.serve_forever()

def init_and_start_task_q_server(serv_port):
    asyncio.run(task_q_server_loop(serv_port))

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--port', dest='port', type=int, default=7777)
    args = parser.parse_args()

    asyncio.run(task_q_server_loop(args.port))

