import asyncio
import queue
import buffer
import json

class TaskQueue:
    def __init__(self):
        self.__q = queue.PriorityQueue(-1)

    def put(self, prior, task):
        self.__q.put((prior, task))
        print('q size={}'.format(self.__q.qsize()))

    def get(self):
        print('q size={}'.format(self.__q.qsize()))
        try:
            prior, task = self.__q.get_nowait()
            return (prior, task)
        except queue.Empty:
            print('queue is empty')
            return (None, None)

class TaskServerProtocol(asyncio.Protocol):
    def __init__(self, task_q):
        print('initing')
        super(asyncio.Protocol, self).__init__()


        self.__recv_buf = buffer.Buffer()


        self.__context = bytearray(b'')
        self.__context_nbytes = 0
        self.__context_len = -1

        self.__task_q = task_q

        print('done init')

    def connection_made(self, transport):
        peername = transport.get_extra_info('peername')
        print('Connection from {}'.format(peername))
        self.transport = transport

    def eof_received(self):
        print('Closing: {}'.format(self.transport))
        self.transport.write('server: done writing'.encode())
        self.transport.close()

    def data_received(self, data):
        print('Data received: {} on {}'.format(data, self.transport))
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

    # TODO: handle context
    def handle_context(self, context):
        print('cmd = {}'.format(context['cmd']))
        print('start handling context')

        if context['cmd'] == 'pull':
            print('try to get task from self.__task_q')
            prior, task = self.__task_q.get()
            print('get task from self.__task_q: task={}(prior={})'.format(task, prior))
            resp = dict()
            resp['cmd'] = 'pulled task'
            resp['body'] = task
            json_resp = json.dumps(resp)
            len_json_resp = len(json_resp)

            self.transport.write(len_json_resp.to_bytes(length=4, byteorder='big', signed=False))
            self.transport.write(json_resp.encode())

        if context['cmd'] == 'push':
            task = context['body']
            self.__task_q.put(task['prior'], task['body'])

            # demo: echo task
            json_ctx = json.dumps(context)
            len_json_ctx = len(json_ctx)
            self.transport.write(len_json_ctx.to_bytes(length=4, byteorder='big', signed=False))
            self.transport.write(json_ctx.encode())

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
