import cv2
import numpy as np

def encode_task(task):
    assert('input_ctx' in task)

    if 'image' in task['input_ctx']:
        img_raw = task['input_ctx']['image']
        task['input_ctx']['image'] = str(cv2.imencode('.jpg', img_raw)[1].tobytes())

    return task

def decode_task(task):
    assert('input_ctx' in task)
    
    if 'image' in task['input_ctx']:
        img_jpg = np.frombuffer(eval(task['input_ctx']['image']), dtype=np.uint8)
        task['input_ctx']['image'] = np.array(cv2.imdecode(img_jpg, cv2.IMREAD_UNCHANGED))

    return task
