import cv2
import numpy as np
from utils import utils
import time
import signal
import os
import abc

class Generator(abc.ABC):
    def __init__(self, video_cap, init_task_q):
        self.__wf_header = None
        self.__video_q = video_cap
        self.__init_task_q = init_task_q
        self.__task_seq = 0

    def register_workflow(self, header):
        # register workflow header
        self.__wf_header = header

    def start(self):
        self.push_task_to_queue()

    @abc.abstractmethod
    def fill_init_task(self, input_ctx, data):
        print('abstract fill_init_task')
        ctx = {}
        return ctx
 
    def generate_init_task(self, frame):
        init_task = {}
        init_task['id'] = self.__task_seq
        init_task['t_init'] = time.time()

        init_task['cur_step'] = 0
        task_name = self.__wf_header['flow'][0]['name']
        init_task['task_name'] = task_name
        # init_task['input_ctx'] = self.fill_init_task(self.__wf_header['input_ctx'][task_name], frame)
        init_task['input_ctx'] = self.fill_init_task(frame)
        print('[{}][{}] made 1 init task'.format(os.getpid(), __name__))

        return init_task

    def push_task_to_queue(self):
        print('[{}] trying to read the first video'.format(os.getpid()))
        assert self.__video_q.isOpened()
        ret, frame = self.__video_q.read()
        if not ret:
            print('[{}] video q not ready'.format(os.getpid()))

        while ret:
            print('[{}] in while'.format(os.getpid()))
            init_task = self.generate_init_task(frame)

            # produce to task_queue (blocking)
            self.__init_task_q.put(init_task)
            self.__task_seq += 1
            print('[{}][{}] produce 1 init_task'.format(os.getpid(), __name__))

            # consume from data queue
            ret, frame = self.__video_q.read()

class PoseEstimationGenerator(Generator):

    def fill_init_task(self, data):
        ctx = {}
        ctx['image'] = data
        return ctx

    def generate_init_task(self, frame):
        print('[{}] specific generate_init_task'.format(__name__))
        return Generator.generate_init_task(self, frame)

'''
Mocking data queue
# video_cap = nano_cam_test.get_nano_videocap()
video_cap = cv2.VideoCapture('input/video/input.mov')

def term_handler(signum, frame):
    if video_cap.isOpened():
        video_cap.release()
        print('closed video_cap')
    print('exiting anyway...')
    exit(1)
signal.signal(signal.SIGINT, term_handler)
'''

def init_and_start_generator_proc(header, init_task_q):

    video_cap = cv2.VideoCapture('input/input1.mp4')

    pose_generator = PoseEstimationGenerator(video_cap, init_task_q)

    pose_generator.register_workflow(header)
    print('[{}] register workflow'.format(os.getpid()))

    pose_generator.start()


if __name__ == '__main__':

    print('generator in main')

