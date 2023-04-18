import cv2
import face_detection
import face_alignment_cnn
import numpy as np
from utils import utils
import time
import os
import functools

import sys
sys.path.append('../..')
import executor

demo_header = {
        'name': 'POSE_ESTIMATION',
        'flow': [
            {'name': 'D'},
            {'name': 'C'}
        ],
        'model_ctx': {
            'D': {
                'net_type': 'mb_tiny_RFB_fd',
                'input_size': 480,
                'threshold': 0.7,
                'candidate_size': 1500,
                # 'device': 'cuda:0'
                'device': 'cpu'
            },
            'C': {
                # 'lite_version': False,
                # 'model_path': 'models/hopenet.pkl',
                'lite_version': True,
                'model_path': 'models/hopenet_lite_6MB.pkl',
                'batch_size': 1,
                # 'device': 'cuda:0'
                'device': 'cpu'
            }   
        },
        'input_ctx': {
            'D': ['image'],
            'C': ['image', 'bbox', 'prob'],
            'R': ['image', 'bbox', 'head_pose']
        },
        'output_ctx': {
            'D': ['image', 'bbox', 'prob'],
            'C': ['image', 'bbox', 'head_pose'],
            'R': []
        }   
    }

class PoseEstimationExecutor(executor.Executor):
    def __init__(self):
        super(PoseEstimationExecutor, self).__init__()
        self.set_load_executor_handler = functools.partial(PoseEstimationExecutor.load_executor, self)

    def load_executor(self, task_name, model_ctx):
        if task_name == 'D':
            executor = face_detection.FaceDetection(model_ctx)
        elif task_name == 'C':
            executor = face_alignment_cnn.FaceAlignmentCNN(model_ctx)
        else:
            print('[{}] unsupported task_name in executor'.format(__name__))
        return executor

'''
video_cap = cv2.VideoCapture('input/input.mov')
 
def get_frame():
    ret, frame = video_cap.read()
    return ret, frame

import signal
def term_handler(signum, frame):
    if video_cap.isOpened():
        video_cap.release()
        print('closed video_cap')
    print('exiting anyway...')
    exit(1)

signal.signal(signal.SIGINT, term_handler)
'''

if __name__ == '__main__':

    import multiprocessing as mp
    import pose_generator
    import cam_web

    res_q = mp.Queue(5)
    ui_proc = mp.Process(target=cam_web.init_and_start_ui_proc,
                         args=(res_q,))
    ui_proc.start()


    local_task_q = mp.Queue(5)
    pose_gen = mp.Process(target=pose_generator.init_and_start_generator_proc,
                                  args=(demo_header, local_task_q,))
    pose_gen.start()


    pose_exec_obj = PoseEstimationExecutor()
    pose_exec_obj.register_workflow(demo_header)


    curr_task = None
    next_task = None

    while True:
        print('[{}] try to get one curr_task'.format(os.getpid()))
        curr_task = local_task_q.get()
        print('[{}] got one curr_task'.format(os.getpid()))

        # step1: D
        next_task = pose_exec_obj.do_task(curr_task)
        print('[{}] done next_task(D)'.format(os.getpid()))

        # step2: C
        next_task = pose_exec_obj.do_task(next_task)
        print('[{}] done next_task(C)'.format(os.getpid()))
        # print('[{}] drop next_task: {}'.format(os.getpid(), next_task))

        # step3: put to web
        res_q.put(next_task)

