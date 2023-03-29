import asyncio
import queue
import buffer
import json
import time
import functools

import protocol_buffer

class TaskQueue:
    def __init__(self):
        self.__q = queue.PriorityQueue(-1)

    def put(self, prior, prior_task):
        print('to put prior={}'.format(prior))
        try:
            self.__q.put_nowait((prior, prior_task))
            return True
        except queue.Full:
            print('queue is full')
            print('q size={}'.format(self.__q.qsize()))
            return False

    def get(self):
        print('q size={}'.format(self.__q.qsize()))
        try:
            prior, prior_task = self.__q.get_nowait()
            return (prior, prior_task)
        except queue.Empty:
            print('queue is empty')
            return (None, None)

    def qsize(self):
        return self.__q.qsize()

class TaskServerProtocol(asyncio.Protocol):
    def __init__(self, task_q):
        print('[{}] initing'.format(__name__))
        super(asyncio.Protocol, self).__init__()

        self.__task_q = task_q

        self.__ptb = protocol_buffer.ProtocolBuffer()
        self.__ptb.set_context_handler(functools.partial(TaskServerProtocol.handle_context, self))

        print('[{}] done init'.format(__name__))

    def connection_made(self, transport):
        peername = transport.get_extra_info('peername')
        print('Connection from {}'.format(peername))
        self.transport = transport

    def eof_received(self):
        print('Closing: {}'.format(self.transport))
        self.transport.write('server: done writing'.encode())
        self.transport.close()

    def data_received(self, data):
        print('Data received (len={}) on {}'.format(len(data), self.transport))
        self.__ptb.parse_and_handle_context(data)

    # TODO: handle context
    def handle_context(self, context):
        print('cmd = {}'.format(context['cmd']))
        print('start handling context')

        if context['cmd'] == 'pull':
            print('try to get prior_task from self.__task_q')
            prior, prior_task = self.__task_q.get()
            if prior_task:
                print('get prior_task from self.__task_q: prior_task(len={})(prior={})'.format(len(prior_task), prior))
            resp = dict()
            resp['cmd'] = 'pulled task'
            resp['body'] = prior_task
            json_resp = json.dumps(resp)
            len_json_resp = len(json_resp)

            self.transport.write(len_json_resp.to_bytes(length=4, byteorder='big', signed=False))
            self.transport.write(json_resp.encode())

        if context['cmd'] == 'push':
            prior_task = context['body']
            print('got one push prior_task(prior={}), pushed to Priority Queue (current qsize = {})'.format(prior_task['prior'], self.__task_q.qsize()))
            ret = self.__task_q.put(prior_task['prior'], prior_task)
            if ret == False:
                resp = dict()
                resp['cmd'] = 'failed'
                json_resp = json.dumps(resp)
                len_json_resp = len(json_resp)

                self.transport.write(len_json_resp.to_bytes(length=4, byteorder='big', signed=False))
                self.transport.write(json_resp.encode())

            # demo: echo task
            # json_ctx = json.dumps(context)
            # len_json_ctx = len(json_ctx)
            # self.transport.write(len_json_ctx.to_bytes(length=4, byteorder='big', signed=False))
            # self.transport.write(json_ctx.encode())

        print('===== done one handling =====\n')


async def main():
    # Get a reference to the event loop as we plan to use
    # low-level APIs.
    loop = asyncio.get_running_loop()

    global_task_q = TaskQueue()

    server = await loop.create_server(
        lambda: TaskServerProtocol(global_task_q),
        '127.0.0.1', 8888)

    print('start awaiting on server...')

    async with server:
        await server.serve_forever()

if __name__ == '__main__':
    asyncio.run(main())
