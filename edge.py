import asyncio
import time
import functools
import multiprocessing as mp
import json
import buffer
import random
import cv2
import argparse

import sys
sys.path.append('test/headup_detect')
import worker_func

from task_client_protocol import TaskClientProtocol
from cloud_server_protocol import CloudServerProtocol
from cloud_client_protocol import CloudClientProtocol

import task_utils

from pose_generator import PoseEstimationGenerator
from pose_worker import PoseEstimationExecutor
from pose_displayer import PoseEstimationDisplayer
import pose_worker

#######################################
# wrapper func: (used by offloader)
#     pull task from task queue server
#######################################
def pull_task_from_task_serv(client_trans):
    request = dict()
 
    request['cmd'] = 'pull'
    request['body'] = None

    json_req = json.dumps(request)
    len_json_req = len(json_req)

    client_trans.write(len_json_req.to_bytes(length=4, byteorder='big', signed=False))
    client_trans.write(json_req.encode())

#######################################
# wrapper func: (used by worker, generator)
#     produce task to task queue server
#######################################
def push_task_to_task_serv(client_trans, cmd, task_body):

    request = dict()
    prior_task = dict()

    prior_task['prior'] = (-1 * task_body['cur_step'], task_body['t_init'])
    prior_task['body'] = task_utils.encode_task(task_body)
    request['cmd'] = cmd
    request['body'] = prior_task

    print('[push_task_to_task_serv] produce_new_task (prior= {})'.format(prior_task['prior']))
    json_req = json.dumps(request)
    len_json_req = len(json_req)

    client_trans.write(len_json_req.to_bytes(length=4, byteorder='big', signed=False))
    client_trans.write(json_req.encode())





#######################################
# (Edge) offloader func:
#     decide where to execute a task
#
# (TODO)
#
#######################################
def edge_offloader_cbk(local_task_q, cloud_cli_trans, prior_task):
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

    print('[{}] Partially offload prior_task(prior={}) to cloud ...(local_task_q.qsize() = {})'.\
          format(__name__, prior_task['prior'], [q.qsize() for q in local_task_q]))
    task = task_utils.decode_task(prior_task['body'])
    task_name = task['task_name']
    if task_name == 'D':
        local_task_q[0].put(task)
    elif task_name == 'C' or task_name == 'R':
        push_task_to_task_serv(cloud_cli_trans, cmd='task', task_body=task)

#######################################
# (Edge) offloader func:
#     offloader main loop
#######################################
async def edge_offloader_loop(local_task_q, cloud_ip, cloud_port, task_q_port):

    loop = asyncio.get_running_loop()

    # connect to cloud server for uploading task
    cloud_cli_trans, cloud_cli_protocol = await loop.create_connection(
        lambda: CloudClientProtocol(),
        cloud_ip, cloud_port
    )
    print('[{}] connect to cloud_ip={} cloud_port={}'.format(__name__, cloud_ip, cloud_port))
    # cloud_cli_trans = None

    # connect to task queue for pulling task
    task_cli_trans, task_cli_protocol = await loop.create_connection(
        lambda: TaskClientProtocol(
            functools.partial(edge_offloader_cbk, local_task_q, cloud_cli_trans)
        ),
        '127.0.0.1', task_q_port
    )

    # TODO: pulling for task every one sec
    while True:
        print('[offloader_loop] try to pull one task')

        await asyncio.sleep(0.03)

        # request to task server
        pull_task_from_task_serv(task_cli_trans)





#######################################
# (Cloud) offloader func:
#     decide where to execute a task
#
# (TODO)
#
#######################################
def cloud_offloader_cbk(local_task_q, prior_task):
    print('[{}] Always offloading prior_task(prior={}) to local ...(local_task_q.qsize() = {})'.\
          format(__name__, prior_task['prior'], [q.qsize() for q in local_task_q]))
    # NOTE: will block
    task = task_utils.decode_task(prior_task['body'])
    task_name = task['task_name']
    if task_name == 'D' or task_name == 'C':
        local_task_q[0].put(task)
    elif task_name == 'R':
        print('[{}] offloading a result'.format(__name__))
        local_task_q[1].put(task)

#######################################
# (Cloud) offloader func:
#     offloader main loop
#######################################
async def cloud_offloader_loop(local_task_q, cloud_port, task_q_port):

    loop = asyncio.get_running_loop()

    # create a cloud server for accepting task
    # WARNING: WHY NO ERROR on stdout when cannot create CloudServerProtocol() ?
    cloud_serv = await loop.create_server(
        lambda: CloudServerProtocol(
            functools.partial(cloud_offloader_cbk, local_task_q)
        ),
        '0.0.0.0', cloud_port
    )
    print('[{}] listen on cloud_port={}'.format(__name__, cloud_port))

    # connect to task queue for pulling task
    task_cli_trans, task_cli_protocol = await loop.create_connection(
        lambda: TaskClientProtocol(
            functools.partial(cloud_offloader_cbk, local_task_q)
        ),
        '127.0.0.1', task_q_port
    )

    # TODO: pulling for task every one sec
    while True:
        print('[cloud_offloader_loop] try to pull one task')
        # await asyncio.sleep(5)

        await asyncio.sleep(0.03)

        # request to task server
        pull_task_from_task_serv(task_cli_trans)





#######################################
# worker func:
#     worker main loop
#######################################
async def worker_loop(exec_obj, local_task_q, task_q_host, task_q_port):

    loop = asyncio.get_running_loop()

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
    print('worker_loop looping...')
    while True:
        print('[worker_loop] try to get task')
        task = local_task_q.get()
        print('[worker_loop] got task (len={})'.format(len(task)))

        # demo: simulate executing for a while
        ret_task = exec_obj.do_task(task)

        # produce next task
        print('[worker_loop] pushing new task')
        push_task_to_task_serv(task_cli_trans, cmd='push', task_body=ret_task)
        print('[worker_loop] pushed new task')





#######################################
# generator func:
#     generator main loop
#######################################
async def generator_loop(gene_obj, task_q_host, task_q_port):
    loop = asyncio.get_running_loop()

    task_cli_trans, task_cli_protocol = await loop.create_connection(
        lambda: TaskClientProtocol(None),
        task_q_host, task_q_port
    )

    video_cap = cv2.VideoCapture('test/headup_detect/input/input.mov') 
    ret, frame = video_cap.read()

    count = 0
    sec_produce_interval = 0.5

    # gene_obj = wzl_fun.Generator(app_info)

    while ret:
        print('[generator_loop] generating init task')
        ret, frame = video_cap.read()
        init_task = gene_obj.generate_init_task(frame)
        assert(init_task)

        print('[generator_loop] pushing init task')
        push_task_to_task_serv(task_cli_trans, cmd='push', task_body=init_task)
        print('[generator_loop] pushed init task, count = {}'.format(count))
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
    print('starting worker_loop...')
    exec_obj = None
    if exec_objname == 'PoseEstimationExecutor':
        print('starting PoseEstimationExecutor')
        exec_obj = PoseEstimationExecutor()
        exec_obj.register_workflow(pose_worker.demo_header)
        # exec_obj.register_workflow(apo_info.get_info())
        print('PoseEstimationExecutor started')
    elif exec_objname == 'PoseEstimationDisplayer':
        print('starting PoseEstimationDisplayer')
        exec_obj = PoseEstimationDisplayer()
        print('PoseEstimationDisplayer started')
    elif exec_objname == 'PoseEstimationGenerator':
        print('staring PoseEstimationGenerator')
        exec_obj = PoseEstimationGenerator(video_cap=None, init_task_q=None)
        exec_obj.register_workflow(pose_worker.demo_header)
        print('PoseEstimationGenerator started')
    else:
        print('[NOT SUPPORT EXEC_OBJNAME]...')

    # begin a loop here ...
    if exec_obj:
        if exec_objname == 'PoseEstimationGenerator':
            asyncio.run(generator_loop(exec_obj, task_q_host, task_q_port))
        elif exec_objname == 'PoseEstimationDisplayer':
            asyncio.run(displayer_loop(exec_obj, local_task_q))
        elif exec_objname == 'PoseEstimationExecutor':
            asyncio.run(worker_loop(exec_obj, local_task_q, task_q_host, task_q_port))
        else:
            print('[NOT SUPPORT EXEC_OBJNAME]...')
    else:
        print('exec_obj is None')





#######################################
# offloader entry:
#    ENTRY for offloader
#    also the ENTRY for edge.py
#######################################
if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument('--side', dest='side', type=str)
    parser.add_argument('--cloud_ip', dest='cloud_ip', type=str)
    parser.add_argument('--cloud_port', dest='cloud_port', type=int, default=9999)
    parser.add_argument('--task_q_port', dest='task_q_port', type=int, default=7777)
    args = parser.parse_args()

    # should create a MULTI-PROCESS queue
    # to deliver task from Offloader to Worker
    local_task_q = [mp.Queue(10), mp.Queue(10)]

    # app_obj = wzl_fun.AppInfo()

    # fork worker(s)
    worker1 = mp.Process(target=worker_main,
                         args=('PoseEstimationExecutor',
                               local_task_q[0],
                               '127.0.0.1', args.task_q_port))
    if args.side == 'e':
        generator1 = mp.Process(target=worker_main,
                                args=('PoseEstimationGenerator',
                                      None,
                                      '127.0.0.1', args.task_q_port))
    if args.side == 'c':
        disp1 = mp.Process(target=worker_main,
                           args=('PoseEstimationDisplayer',
                                 local_task_q[1],
                                 None, None))
    worker1.start()
    if args.side == 'e':
        generator1.start()
    if args.side == 'c':
        disp1.start()

    # run offloader

    if args.side == 'e' and args.cloud_ip and args.cloud_port:
        asyncio.run(edge_offloader_loop(local_task_q, args.cloud_ip, args.cloud_port, args.task_q_port))
    elif args.side == 'c':
        asyncio.run(cloud_offloader_loop(local_task_q, args.cloud_port, args.task_q_port))
    else:
        print('[{}] ERROR cannot start offloader_loop'.format(__name__))
        worker1.terminate()
        exit(1)

    print('out of asyncio')
