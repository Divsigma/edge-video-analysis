import sys
import cv2
import face_detection
import face_alignment_cnn
import numpy as np
from utils import utils
import time
import os

class Executor():
    def __init__(self):
        self.__wf_header = None
        self.__executor = dict()
        self.__load_executor_handler = self.load_executor

    def set_load_executor_handler(self, cbk):
        self.__load_executor_handler = cbk

    def register_workflow(self, header):
        # register task header
        self.__wf_header = header

        # load task executor
        model_ctx = self.__wf_header['model_ctx']
        for flow_node in header['flow']:
            task_name = flow_node['name']
            self.__executor[task_name] = self.__load_executor_handler(task_name, model_ctx[task_name])

    def wrap_next_task(self, task, output_ctx):
        ret_task = None

        task_name = task['task_name']
        cur_step = task['cur_step']
        wf = self.__wf_header['flow']

        if cur_step + 1 == len(wf):
            res_task = {}
            res_task['id'] = task['id']
            res_task['t_init'] = task['t_init']
            res_task['cur_step'] = task['cur_step'] + 1
            res_task['task_name'] = 'R'
            res_task['t_end'] = time.time()
            res_task['input_ctx'] = output_ctx
            print('task-{} come to an end'.format(task['id']))
            print('task-{} t_consume={}, t_init={}, t_end={}'.format(
                  task['id'], res_task['t_end'] - res_task['t_init'], res_task['t_init'], res_task['t_end']))

            print('produce 1 res_task for task-{}'.format(task['id']))
            ret_task = res_task

        else:
            next_task_name = self.__wf_header['flow'][cur_step + 1]['name']
            if next_task_name == 'C' or next_task_name == 'D':
                next_task = {}
                next_task['id'] = task['id']
                next_task['t_init'] = task['t_init']
                next_task['cur_step'] = task['cur_step'] + 1
                next_task['task_name'] = next_task_name
                next_task['input_ctx'] = output_ctx
                print('made 1 next_task({}) for task-{}'.format(next_task_name, task['id']))

                print('produce 1 next_task({}) for task-{}'.format(next_task_name, task['id']))
                ret_task = next_task

            else:
                print('unsupported next_task_name({})'.format(next_task_name))

        return ret_task


    def do_task(self, task):
        next_task = None

        task_name = task['task_name']
        input_ctx = task['input_ctx']

        # should not receive result as a task
        cur_step = task['cur_step']
        wf = self.__wf_header['flow']
        assert(cur_step < len(wf))
                
        if task_name == 'D' or task_name == 'C':
            print('[{}][{}] executing task_name({})'.format(os.getpid(), __name__, task_name))
            output_ctx = self.__executor[task_name](input_ctx)

            # wrap next task
            next_task = self.wrap_next_task(task, output_ctx)

        else:
            print('[{}][{}] unsupported task_name({})'.format(os.getpid(), __name__, task_name))

        return next_task


if __name__ == '__main__':

    print('executor in main')
