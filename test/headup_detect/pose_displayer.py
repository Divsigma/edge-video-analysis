import functools
import cam_web

import sys
sys.path.append('../..')
import executor


class PoseEstimationDisplayer(executor.Executor):
    def __init__(self):
        super(PoseEstimationDisplayer, self).__init__()
        self.set_load_executor_handler = functools.partial(PoseEstimationDisplayer.load_executor, self)

    def load_executor(self, task_name, model_ctx):
        return None

    def start_displaying(self, res_q):
        cam_web.init_and_start_ui_proc(res_q)

'''
import cv2
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
