import asyncio
import time
import functools
import multiprocessing as mp
import json
import buffer
import random

from task_client_protocol import TaskClientProtocol
# from cloud_client_protocol import CloudClientProtocol

class CloudClientProtocol(asyncio.Protocol):
    def connection_made(self, transport):
        print('upload connection made, transport = {}'.format(transport))

    def data_received(self, data):
        print('Got data: {}'.format(data))

#######################################
# offloader func:
#     decide where to execute a task
#
# (TODO)
#
#######################################
def offloader_cbk(local_task_q, cloud_cli_trans, task):
    print('[{}] Deciding where to offload ... task={} (local_task_q.qsize() = {})'.\
          format(__name__, task, local_task_q.qsize()))
    # NOTE: will block
    local_task_q.put(task)

#######################################
# offloader func:
#     offloader main loop
#######################################
async def offloader_main(local_task_q):

    loop = asyncio.get_running_loop()

    # connect to cloud server for uploading task
    cloud_cli_trans, cloud_cli_protocol = await loop.create_connection(
        lambda: CloudClientProtocol(),
        '127.0.0.1',
        9999
    )

    # connect to task queue for pulling task
    task_cli_trans, task_cli_protocol = await loop.create_connection(
        lambda: TaskClientProtocol(
            functools.partial(offloader_cbk, local_task_q, cloud_cli_trans)
        ),
        '127.0.0.1',
        8888
    )

    # TODO: pulling for task every one sec
    ti = 100
    while True:
        request = dict()
        task = dict()

        ti = ti - 1
        await asyncio.sleep(1)

        request['cmd'] = 'pull'
        request['body'] = None
        json_req = json.dumps(request)
        len_json_req = len(json_req)

        task_cli_trans.write(len_json_req.to_bytes(length=4, byteorder='big', signed=False))
        task_cli_trans.write(json_req.encode())

#######################################
# worker func:
#     produce task to task queue server
#######################################
async def produce_task(client_trans, task_body):

    request = dict()
    task = dict()

    task['prior'] = random.randint(1, 100)
    task['body'] = task_body
    request['cmd'] = 'push'
    request['body'] = task

    print('[worker_loop] produce_new_task {}'.format(__name__, request))
    json_req = json.dumps(request)
    len_json_req = len(json_req)

    client_trans.write(len_json_req.to_bytes(length=4, byteorder='big', signed=False))
    client_trans.write(json_req.encode())

#######################################
# worker func:
#     worker main loop
#######################################
async def worker_loop(local_task_q, task_q_host, task_q_port):

    loop = asyncio.get_running_loop()

    # connect to task server for pushing task
    task_cli_trans, task_cli_protocol = await loop.create_connection(
        lambda: TaskClientProtocol(
            functools.partial(offloader_cbk, local_task_q, None)
        ),
        task_q_host,
        task_q_port
    )

    # demo: produce an init task (should be done by Generator)
    await produce_task(task_cli_trans,
                       task_body='work_main init task x32$@k..')
    print('worker_loop pushed init task')

    # worker only pull task from local q (a MULTI-PROCESS queue)
    print('worker_loop looping...')
    while True:
        print('[worker_loop] try to get task')
        task = local_task_q.get()
        print('[worker_loop] got task = {}'.format(task))

        # demo: simulate executing for a while
        await asyncio.sleep(4)

        # produce next task
        task_body = random.randint(1000, 5000)
        print('[worker_loop] pushing new task, task_body = {}'.format(task_body))
        await produce_task(task_cli_trans,
                           task_body='work_main new task {}'.format(task_body))
        print('[worker_loop] pushed new task, task_body = {}'.format(task_body))


#######################################
# worker entry:
#     ENTRY for worker sub-process
#######################################
def worker_main(local_task_q, task_q_host, task_q_port):
    print('starting worker_loop...')
    asyncio.run(worker_loop(local_task_q, task_q_host, task_q_port))





#######################################
# MAIN 
#######################################
if __name__ == '__main__':

    # should create a MULTI-PROCESS queue
    # to deliver task from Offloader to Worker
    local_task_q = mp.Queue(10)

    # fork worker(s)
    worker1 = mp.Process(target=worker_main,
                         args=(local_task_q, '127.0.0.1', 8888))
    worker1.start()

    # run offloader
    asyncio.run(offloader_main(local_task_q))
    print('out of asyncio')
