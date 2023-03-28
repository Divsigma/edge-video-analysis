import sys
import cv2
import face_detection
import face_alignment_cnn
import numpy as np
from utils import utils
import time
import os
import worker_func
import multiprocessing as mp
import functools
import pose_generator
import cam_web

class PoseEstimationDisplayer(worker_func.Executor):
    def __init__(self):
        super(PoseEstimationDisplayer, self).__init__()
        self.set_load_executor_handler = functools.partial(PoseEstimationDisplayer.load_executor, self)

    def load_executor(self, task_name, model_ctx):
        executor = None
        return executor

    def start_displaying(self, res_q):
        cam_web.init_and_start_ui_proc(res_q)

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
    print('pose_displayer.py in main')
