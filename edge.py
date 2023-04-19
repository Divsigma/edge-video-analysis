import asyncio
import time
import functools
import multiprocessing as mp
if __name__ == '__main__':
    mp.set_start_method('spawn')
import json
import buffer
import random
import cv2
import argparse

import sys
import executor

import task_server_protocol
from task_client_protocol import TaskClientProtocol
from cloud_server_protocol import CloudServerProtocol
from cloud_client_protocol import CloudClientProtocol

import task_utils
from logging_utils import root_logger

sys.path.append('test/headup_detect')
from pose_generator import PoseEstimationGenerator
from pose_worker import PoseEstimationExecutor
from pose_displayer import PoseEstimationDisplayer
import pose_worker

#######################################
# wrapper func: (used by offloader)
#     pull task from task queue server
#######################################
def pull_task_from_task_serv(client_protocol):
    request = dict()
 
    request['cmd'] = 'pull'
    request['body'] = None

    client_protocol.write_context(request)

#######################################
# wrapper func: (used by worker, generator)
#     produce task to task queue server
#######################################
def push_task_to_task_serv(client_protocol, cmd, task_body):

    request = dict()
    prior_task = dict()

    prior_task['prior'] = (-1 * task_body['cur_step'], task_body['t_init'])
    prior_task['body'] = task_utils.encode_task(task_body)
    request['cmd'] = cmd
    request['body'] = prior_task

    root_logger.info('produce_new_task (cmd = {}, prior= {})'.format(cmd, prior_task['prior']))

    client_protocol.write_context(request)





#######################################
# (Edge) offloader func:
#     decide where to execute a task
#
# (TODO)
#
#######################################
def edge_offloader_cbk(local_task_q, cloud_cli_protocol, prior_task):
    # print('[{}] Always offloading prior_task(prior={}) to local ...(local_task_q.qsize() = {})'.\
    #       format(__name__, prior_task['prior'], [q.qsize() for q in local_task_q]))

    # NOTE: will block
    # task = task_utils.decode_task(prior_task['body'])
    # task_name = task['task_name']
    # if task_name == 'D' or task_name == 'C':
    #     local_task_q[0].put(task)
    # elif task_name == 'R':
    #     print('[{}] offloading a result'.format(__name__))
    #     local_task_q[1].put(task)

    # print('[{}] Always offloading prior_task(prior={}) to cloud ...(local_task_q.qsize() = {})'.\
    #       format(__name__, prior_task['prior'], [q.qsize() for q in local_task_q]))
    # task = task_utils.decode_task(prior_task['body'])
    # push_task_to_task_serv(cloud_cli_trans, cmd='task', task_body=task)

    root_logger.info('partially offload prior_task (prior = {}) to CLOUD, local_task_q.qsize() = {}'.\
                     format(prior_task['prior'], [q.qsize() for q in local_task_q]))
    task = task_utils.decode_task(prior_task['body'])
    task_name = task['task_name']
    if task_name == 'D' or task_name == 'C':
        local_task_q[0].put(task)
    elif task_name == 'R':
        push_task_to_task_serv(cloud_cli_protocol, cmd='task', task_body=task)

#######################################
# (Edge) offloader func:
#     offloader main loop
#######################################
async def edge_offloader_loop(cloud_ip, cloud_port, task_q_port):

    # should create a MULTI-PROCESS queue
    # to deliver task from offloader to worker
    local_task_q = [mp.Queue(10), mp.Queue(10)]

    # fork worker(s)
    worker1 = mp.Process(target=worker_main,
                         args=('PoseEstimationExecutor',
                               local_task_q[0],
                               '127.0.0.1', task_q_port))
    generator1 = mp.Process(target=worker_main,
                            args=('PoseEstimationGenerator',
                                  None,
                                  '127.0.0.1', task_q_port))
    # start task queue server
    # NOTES: should ensure task queue server is started
    #        before offloader trying to connect
    task_q_server = mp.Process(target=task_server_protocol.init_and_start_task_q_server,
                               args=(task_q_port,))
    task_q_server.start()
    await asyncio.sleep(2)


    # start worker(s)
    worker1.start()
    generator1.start()


    # for Python3.6
    loop = asyncio._get_running_loop()

    # connect to cloud server for uploading task
    cloud_cli_trans, cloud_cli_protocol = await loop.create_connection(
        lambda: CloudClientProtocol(),
        cloud_ip, cloud_port
    )
    root_logger.info('connected to cloud_ip = {}, cloud_port = {}'.format(cloud_ip, cloud_port))
    # cloud_cli_trans = None

    # connect to task queue for pulling task
    task_cli_trans, task_cli_protocol = await loop.create_connection(
        lambda: TaskClientProtocol(
            functools.partial(edge_offloader_cbk, local_task_q, cloud_cli_protocol)
        ),
        '127.0.0.1', task_q_port
    )
    root_logger.info('connect to task_q_server')

    # TODO: pulling for task every one sec
    t = time.time()
    worker_count = 1
    while True:
        root_logger.info('try to pull one task, worker_count = {}'.format(worker_count))

        await asyncio.sleep(0.03)

        # request to task server
        pull_task_from_task_serv(task_cli_protocol)

        # if time.time() - t > 5 and worker_count < 3:
        #      workerx = mp.Process(target=worker_main,
        #                           args=('PoseEstimationExecutor',
        #                                 local_task_q[0],
        #                                 '127.0.0.1', task_q_port))
        #      workerx.start()
        #      worker_count += 1
        #      t = time.time()
            





#######################################
# (Cloud) offloader func:
#     decide where to execute a task
#
# (TODO)
#
#######################################
def cloud_offloader_cbk(local_task_q, prior_task):
    root_logger.info('always offloading prior_task (prior = {}) to LOCAL, local_task_q.qsize() = {}'.\
                     format(prior_task['prior'], [q.qsize() for q in local_task_q]))
    # NOTE: will block
    task = task_utils.decode_task(prior_task['body'])
    task_name = task['task_name']
    if task_name == 'D' or task_name == 'C':
        local_task_q[0].put(task)
    elif task_name == 'R':
        root_logger.info('offloading a result')
        local_task_q[1].put(task)

#######################################
# (Cloud) offloader func:
#     offloader main loop
#######################################
class CloudServerFactory():
    def __init__(self, local_task_q):
        self.__protocol_list = list()
        self.__local_task_q = local_task_q

    def make_protocol(self):
        new_protocol = CloudServerProtocol(functools.partial(cloud_offloader_cbk, self.__local_task_q))
        self.__protocol_list.append(new_protocol)
        return new_protocol

    def print_protocol(self):
        # print('{}'.format(self.__protocol_list))
        for p in self.__protocol_list:
            print('peername={}'.format(p._CloudServerProtocol__trans.get_extra_info('peername')))

async def cloud_offloader_loop(cloud_port, task_q_port):

    root_logger.warning('loop={}'.format(asyncio._get_running_loop()))

    # should create a MULTI-PROCESS queue
    # to deliver task from offloader to worker
    local_task_q = [mp.Queue(10), mp.Queue(10)]

    # fork worker(s)
    worker1 = mp.Process(target=worker_main,
                         args=('PoseEstimationExecutor',
                               local_task_q[0],
                               '127.0.0.1', task_q_port))
    disp1 = mp.Process(target=worker_main,
                       args=('PoseEstimationDisplayer',
                             local_task_q[1],
                             None, None))

    # start task queue server
    # NOTES: should ensure task queue server is started
    #        before offloader trying to connect
    task_q_server = mp.Process(target=task_server_protocol.init_and_start_task_q_server,
                               args=(task_q_port,))
    task_q_server.start()
    await asyncio.sleep(2)

    # start worker(s)
    worker1.start()
    disp1.start()


    # for Python3.6
    loop = asyncio._get_running_loop()

    # create a cloud server for accepting task
    # WARNING: WHY NO ERROR on stdout when cannot create CloudServerProtocol() ?
    cloud_server_factory = CloudServerFactory(local_task_q)
    await loop.create_server(cloud_server_factory.make_protocol, '0.0.0.0', cloud_port)
    # cloud_serv = await loop.create_server(
    #     lambda: CloudServerProtocol(
    #         functools.partial(cloud_offloader_cbk, local_task_q)
    #     ),
    #     '0.0.0.0', cloud_port
    # )
    root_logger.info('listening on cloud_port = {}'.format(cloud_port))

    # connect to task queue for pulling task
    root_logger.info('try to connect 127.0.0.1:{}'.format(task_q_port))
    task_cli_trans, task_cli_protocol = await loop.create_connection(
        lambda: TaskClientProtocol(
            functools.partial(cloud_offloader_cbk, local_task_q)
        ),
        '127.0.0.1', task_q_port
    )
    root_logger.info('connect to task_q_server')

    # TODO: pulling for task every one sec
    while True:
        root_logger.info('try to pull one task')
        cloud_server_factory.print_protocol()

        await asyncio.sleep(5)
        # await asyncio.sleep(0.03)

        # request to task server
        # pull_task_from_task_serv(task_cli_protocol)





#######################################
# worker func:
#     worker main loop
#######################################
async def worker_loop(exec_obj, local_task_q, task_q_host, task_q_port):

    # for Python3.6
    loop = asyncio._get_running_loop()

    # connect to task server for pushing task
    task_cli_trans, task_cli_protocol = await loop.create_connection(
        lambda: TaskClientProtocol(None),
        task_q_host, task_q_port
    )

    # demo: produce an init task (should be done by Generator)
    # await produce_task(task_cli_trans,
    #                    task_body='work_main init task x32$@k..')
    # print('worker_loop pushed init task')

    # worker only pull task from local q (a MULTI-PROCESS queue)
    root_logger.info('worker_loop looping...')
    while True:
        root_logger.info('try to get task from LOCAL')
        task = local_task_q.get()
        root_logger.info('got task from LOCAL (len={})'.format(len(task)))

        # demo: simulate executing for a while
        ret_task = exec_obj.do_task(task)

        # produce next task
        root_logger.info('pushing new task')
        push_task_to_task_serv(task_cli_protocol, cmd='push', task_body=ret_task)





#######################################
# generator func:
#     generator main loop
#######################################
async def generator_loop(gene_obj, task_q_host, task_q_port):
    loop = asyncio._get_running_loop()

    task_cli_trans, task_cli_protocol = await loop.create_connection(
        lambda: TaskClientProtocol(None),
        task_q_host, task_q_port
    )

    # video_cap = cv2.VideoCapture('test/headup_detect/input/input.mov') 
    video_cap = cv2.VideoCapture('test/headup_detect/input/input1.mp4') 
    ret, frame = video_cap.read()

    count = 0
    sec_produce_interval = 0.5

    # gene_obj = wzl_fun.Generator(app_info)

    while ret:
        root_logger.info('generating init task')
        ret, frame = video_cap.read()
        init_task = gene_obj.generate_init_task(frame)
        assert(init_task)

        root_logger.info('pushing init task')
        push_task_to_task_serv(task_cli_protocol, cmd='push', task_body=init_task)
        root_logger.info('pushed init task, count = {}'.format(count))
        count = count + 1

        await asyncio.sleep(sec_produce_interval)

        ret, frame = video_cap.read()





#######################################
# displayer func:
#     displayer main loop
#######################################
async def displayer_loop(disp_obj, local_task_q):
    disp_obj.start_displaying(local_task_q)




    
#######################################
# worker entry:
#     ENTRY for worker sub-process
#######################################
def worker_main(exec_objname, local_task_q, task_q_host, task_q_port):
    exec_obj = None
    root_logger.info('starting {}'.format(exec_objname))
    if exec_objname == 'PoseEstimationExecutor':
        exec_obj = PoseEstimationExecutor()
        exec_obj.register_workflow(pose_worker.demo_header)
        # exec_obj.register_workflow(apo_info.get_info())
    elif exec_objname == 'PoseEstimationDisplayer':
        exec_obj = PoseEstimationDisplayer()
    elif exec_objname == 'PoseEstimationGenerator':
        exec_obj = PoseEstimationGenerator(video_cap=None, init_task_q=None)
        exec_obj.register_workflow(pose_worker.demo_header)
    else:
        root_logger.warning('not support exec_objname')

    # begin a loop here ...
    if exec_obj:
        root_logger.info('exec_obj {} started'.format(exec_objname))
        if exec_objname == 'PoseEstimationGenerator':
            # for Python3.6
            loop = asyncio.get_event_loop()
            loop.run_until_complete(asyncio.wait(
                [generator_loop(exec_obj, task_q_host, task_q_port)]
            ))
            loop.run_forever()

            # for Python3.7+
            # asyncio.run(generator_loop(exec_obj, task_q_host, task_q_port))

        elif exec_objname == 'PoseEstimationDisplayer':
            # for Python3.6
            loop = asyncio.get_event_loop()
            loop.run_until_complete(asyncio.wait(
                [displayer_loop(exec_obj, local_task_q)]
            ))
            loop.run_forever()

            # for Python3.7+
            # asyncio.run(displayer_loop(exec_obj, local_task_q))

        elif exec_objname == 'PoseEstimationExecutor':
            # for Python3.6
            loop = asyncio.get_event_loop()
            loop.run_until_complete(asyncio.wait(
                [worker_loop(exec_obj, local_task_q, task_q_host, task_q_port)]
            ))
            loop.run_forever()

            # for Python3.7+
            # asyncio.run(worker_loop(exec_obj, local_task_q, task_q_host, task_q_port))

        else:
            root_logger.warning('not support exec_objname')
    else:
        root_logger.warning('exec_obj is None')





#######################################
# offloader entry:
#    ENTRY for offloader
#    also the ENTRY for edge.py
#######################################
if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument('--side', dest='side', type=str, required=True)
    parser.add_argument('--cloud_ip', dest='cloud_ip', type=str, default='192.168.56.102')
    parser.add_argument('--cloud_port', dest='cloud_port', type=int, default=9999)
    parser.add_argument('--task_q_port', dest='task_q_port', type=int, default=7777)
    args = parser.parse_args()

    if args.side == 'e':
        args.task_q_port = 8888
    if args.side == 'c':
        args.task_q_port = 7777

    # run offloader
    if args.side == 'e' and args.cloud_ip and args.cloud_port:
        # for Python3.6
        loop = asyncio.get_event_loop()
        loop.run_until_complete(asyncio.wait(
            [edge_offloader_loop(args.cloud_ip, args.cloud_port, args.task_q_port)]
        ))
        loop.run_forever()

        # for Python3.7+
        # asyncio.run(edge_offloader_loop(args.cloud_ip, args.cloud_port, args.task_q_port))

    elif args.side == 'c':
        # for Python3.6
        loop = asyncio.get_event_loop()
        loop.run_until_complete(asyncio.wait(
            [cloud_offloader_loop(args.cloud_port, args.task_q_port)]
        ))
        loop.run_forever()

        # for Python3.7+
        # asyncio.run(cloud_offloader_loop(args.cloud_port, args.task_q_port))

    else:
        root_logger.info('[ERROR] cannot start offloader_loop')
        worker1.terminate()
        exit(1)

    print('out of asyncio')
