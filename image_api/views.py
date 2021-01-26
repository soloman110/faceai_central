import json
import multiprocessing
import logging
from image_api.utils import *
from uuid import uuid1
from django.views.decorators.csrf import csrf_exempt
from django_redis import get_redis_connection
from image_api.FieldValidators import MetainfoValidator, ImageinfoValidator
from enum import Enum

logger = logging.getLogger('api.custom')
logger_pedding = logging.getLogger('pedding.custom')

#Redis에 Metainfo를 저장하는 Key의 prefix
META_RESULT_PREFIX = 'm_'
#Redis에 이미지 정보를 저장하는 Key의 prefix
FTP_IMAGE_PREFIX = 'ftp_img:'
#Redis에 순서대로 Task정보를 저장하는 queue
TASK_QUEUE_PREFIX = 'task_queue'
#Redis에 처리중인 Task정보를 저장하는 queue
PEDDING_TASK_ZSET = 'pedding_task_zset'
#Redis에 Task정보를 저장하는 Key의 Prefix
TASK_INFO_PREFIX = 'taskinfo:'

#해당 시간내에 task를 처리 못하면 abort를 한다
PEDDING_TASK_AGE = 60 * 20 #20분
# TASK정보 보유 기간
TASK_INFO_AGE = 60 * 60 * 24 # 24 Hours
# 이미지 정보 보유 기간
IMG_INFO_AGE = 60 * 60 * 24 # 24 Hours
# 메타 정보 보유 기간
META_RESULT_AGE = 60 * 60 * 24 # 24 Hours

class STATUS(Enum):
    """ Task 처리 상태
    CREATE: 생성됨
    PENDDING: 처리 중
    COMPLETE: 완성
    ABORT: 장기간 동안 처리를 못하여 강제로 작업을 종료됨
    EMPTY: 해당 task정보 없음
    """
    CREATE = 0
    PENDDING = 1
    COMPLETE = 2
    ABORT = 3
    EMPTY = 4

@csrf_exempt
def create_ftpimginfo(request):
    """ IF-FACEAI-001: Client(단말기)가 FTP서버에 업로드한 이미지정보를 전달받는 함수

    Client가 FTP서버로 이미지를 업로드 후 해당 이미지 정보를 전달한다. 어떤 ftp서버에 어떤 파일을 올렸는지를 전달
    구현 로직:
        1. 이미지 정보생성 및 저장
            - 데이터 형식 Hash--> key = ftp_img:id
        2. token에 해당하는 task정보 생성 및 저장.
        3. Ftpid에 해당하는 task_queue에 task정보를 추가

    주의: 이미지 및 task정보는 TASK_INFO_AGE 시간 후 삭제됨 (Redis expire)

    Args:
        request (HTTP REQUEST):
            - method: POST
            - ftpid를 default값 1

    Returns: token를 리턴

    """
    if request.method == 'POST':
        valid_ser = ImageinfoValidator(data=request.POST)
        if not valid_ser.is_valid():
            return json_error('Data Invalid', code=422, data=valid_ser.errors)

        conn = get_redis_connection('default')
        token = create_token()
        imglist = request.POST['imglist']
        ftpid = request.POST.get('ftpid', '1')
        imglist = json.loads(imglist)
        logger.info("%s token: %s, imgid_list: %s", 'IF-FACEAI-001', ftpid, imglist)

        #1. 이미지 정보 저장
        imgid_list = save_imginfos(conn,ftpid, imglist)
        # 2. token에 해당하는 taskinfo 저장, expire시간 지정
        save_taskinfo(conn, token, imgid_list)
        #3. Ftpid에 해당하는 task_queue에 task정보를 추가
        push_task(conn, ftpid, token)

        return json_response({'token': token})
    else:
        return json_error('HTTP METHOD ERROR', 405)

@csrf_exempt
def task_provider(request):

    """ IF-FACEAI-002: FTP서버에서 처리할 이미지 정보를 제공하는 함수; 큐(Redis List)를 통해 단말기로 부터 전달받은 이미지 정보를 순서대로 제공

    구현 로직은 다음과 같다:
    1. ftpid에 해당하는 큐(TASK_QUEUE_PREFIX + ftpid)에서 task 정보를 얽어온다
    2. 읽어온 tasks를 pedding_task큐에 저장한다. (예외처리 용도)
    3. Task의 Status를 STATUS.PENDDING로 업데이트
    3. Pipeline방식으로 tasks에 속한 img 정보를 한꺼번에 읽어 온다
    4. 읽어온 img정보로 json data생성

    Args:
        request (HTTP REQUEST):
            - limit: 읽어올 개수 지정 max=1000; defqult value=100
            - ftpid: default value = 1
    Returns: token별 이미지 리스트 정보

    """
    ftpid = request.GET.get('ftpid', '1')
    limit = int(request.GET.get('limit', '100'))
    limit = min(1000, limit)
    #logger.info("%s ftpid %s, limit %s", 'IF-FACEAI-002', ftpid, limit)

    conn = get_redis_connection('default')
    # 1. ftpid에 해당하는 큐(TASK_QUEUE_PREFIX + ftpid)에서 task 정보를 얽어온다
    tasklist = pop_task(conn, ftpid, limit)

    # 2. 읽어온 tasks를 pedding_task큐에 저장한다
    add_peddingtask(conn, tasklist)

    #3 Task의 Status를 STATUS.PENDDING로 업데이트
    update_status_tasklist(conn, tasklist, STATUS.PENDDING.value)

    # 4. Pipeline방식으로 tasks에 속한 img 정보를 한꺼번에 읽어 온다
    imglist = imglist_pipelie(conn, tasklist)
    # 5. 읽어온 img정보로 json data생성
    result_list = []
    start_index = 0
    for taskinfo in tasklist:
        imgids = taskinfo.get('imgstr').split('#')
        end_index = start_index + len(imgids)
        info = {}
        info['token'] = taskinfo.get('token')
        info['imglist'] = imglist[start_index : end_index]
        result_list.append(info)
        start_index = end_index

    return json_response(result_list)

@csrf_exempt
def get_peddingtasks(request):
    """ IF-FACEAI-003: 처리된 이미지 meta정보를 저장하는 함수
    """
    limit = request.GET.get('limit', 100)
    conn = get_redis_connection('default')
    pedding_tasks = conn.zrange(PEDDING_TASK_ZSET, 0, int(limit), desc=False, withscores=True)
    #timestamp를 datetime로 변경
    #pedding_tasks = list(map(lambda task: (task[0], timetamp_formatter(task[1])), pedding_tasks))
    pedding_tasks = list(map(lambda task: {'token': task[0], 'createtime': timetamp_formatter(task[1])}, pedding_tasks))
    return json_response(pedding_tasks)

@csrf_exempt
def metainfo(request):
    """ IF-FACEAI-004: 처리된 이미지 meta정보를 저장하는 함수

    처리 로직
    1. Redis Map에다 metainfo를 저장 (HashMap-> key:token, value: json_list)
    2. Task의 Status를 STATUS.COMPLETE로 업데이트
    3. 처리된 token정보를 pedding_task에서 삭제

    Args: request (HTTP REQUEST):
            - token 필수
            - metainfos (json array):

    Returns: json_result

    """
    valid_ser = MetainfoValidator(data=request.POST)
    if valid_ser.is_valid():
        token = request.POST['token']
        metainfos = request.POST['metainfos']
        logger.info("%s token: %s", 'IF-FACEAI-004', token)

        conn = get_redis_connection('default')
        meta_result_key = META_RESULT_PREFIX + token
        conn.set(meta_result_key, metainfos)
        conn.expire(meta_result_key + token, META_RESULT_AGE)  # 24시간 후 삭제
        update_status_task(conn, token, STATUS.COMPLETE.value)
        conn.zrem(PEDDING_TASK_ZSET, token)
        return json_response({})
    else:
        logger.error("Data Invalid [detail_info]: %s", valid_ser.errors)
        return json_error('Data Invalid', code=422, data=valid_ser.errors)

@csrf_exempt
def info(request):
    """ IF-FACEAI-005: token에 해당하는 meta 정보 제공
    주의: token의 유효 시간은 META_RESULT_AGE에서 설정
    """
    token = request.GET['token']
    status = get_status(token)
    logger.info("%s token: %s, status: %s", 'IF-FACEAI-005', token, status)

    if str(STATUS.COMPLETE.value) != status:
        return json_response([], status=status)
    else:
        metainfo = get_info(token)
        metainfo = metainfo.replace("\'", "\"")
        return json_response(json.loads(metainfo), status=status)

#Pipline방식으로 Redis List에서 item를 여러개를 한꺼번에 읽어오다
def multi_pop(r, q, n):
  p = r.pipeline()
  p.multi()
  p.lrange(q, 0, n - 1)
  p.ltrim(q, n, -1)
  return p.execute()

#이미지 정보를 한꺼번에 읽어오다.
def imglist_pipelie(conn, tasklist):
    pipline = conn.pipeline(False)
    for task in tasklist:
        imgids = task.get('imgstr').split('#')
        for imgid in imgids:
            pipline.hgetall(FTP_IMAGE_PREFIX + imgid)
    imglist = pipline.execute()
    return imglist

def tasklist_pipelie(conn, tasklist):
    pipline = conn.pipeline(False)
    for taskinfo_hashkey in tasklist:
        pipline.hgetall(taskinfo_hashkey)
    tasklist = pipline.execute()
    return tasklist

#빈 스트림 여부를 체크하는 함수.
def isEmpty(str):
    if str == '' or str is None:
        return True
    return False

def create_token():
    return str(uuid1())

def redis_hemset(conn, hashname, mappings, expire=None):
    conn.hmset(hashname, mappings)
    if expire is not None:
        conn.expire(hashname, expire)  # 24시간 후 삭제

def save_imginfo(conn, ftpid, img):
    img_id = str(conn.incr('ftp_img:'))
    now = time.time()
    imginfo = FTP_IMAGE_PREFIX + img_id
    redis_hemset(conn, imginfo, {
        'id': img_id,
        'path': img['path'],
        'ftpid': ftpid,
        'create_time': now
    }, IMG_INFO_AGE)
    return img_id

def save_imginfos(conn, ftpid, imglist):
    imgid_list = []
    for img in imglist:
        img_id = save_imginfo(conn, ftpid, img)
        imgid_list.append(img_id)
    return imgid_list

def save_taskinfo(conn, token, imgid_list):
    imgstr = '#'.join([str(el) for el in imgid_list])
    taskinfo = TASK_INFO_PREFIX + token
    conn.hmset(taskinfo, {
        'token': token,
        'imgstr': imgstr,
        'create_time': time.time(),
        'status': STATUS.CREATE.value
    })
    conn.expire(taskinfo, TASK_INFO_AGE)

def push_task(conn, ftpid, token):
    taskinfo_key = TASK_INFO_PREFIX + token
    conn.rpush(TASK_QUEUE_PREFIX + ftpid, taskinfo_key)

def pop_task(conn, ftpid, limit):
    result = multi_pop(conn, TASK_QUEUE_PREFIX + ftpid, limit)
    k_list = list(filter(None, result[0]))
    tasklist = tasklist_pipelie(conn, k_list)
    return tasklist

def add_peddingtask(conn, tasklist):
    if len(tasklist) > 0:
        # conn.rpush(PEDDING_TASK_QUEUE_PREFIX + ftpid, *tasklist)
        timestamp = time.time()
        for task in tasklist:
            token = task.get('token')
            conn.zadd(PEDDING_TASK_ZSET, {token: timestamp})

def update_status_tasklist(conn, tasklist, status):
    pipline = conn.pipeline(False)
    for task in tasklist:
        token = task.get('token')
        pipline.hset(TASK_INFO_PREFIX + token, 'status', status)
    pipline.execute()

def update_status_task(conn, token, status):
    pipline = conn.pipeline(False)
    pipline.hset(TASK_INFO_PREFIX + token, 'status', status)
    pipline.execute()

def get_info(token):
    conn = get_redis_connection('default')
    metainfo = conn.get(META_RESULT_PREFIX + token)
    return metainfo

def get_status(token):
    conn = get_redis_connection('default')
    status = conn.hget(TASK_INFO_PREFIX + token, 'status')
    if status is None:
        return STATUS.EMPTY.value
    return status

def get_taskinfo(token):
    conn = get_redis_connection('default')
    taskinfo = conn.hgetall(TASK_INFO_PREFIX + token)
    return taskinfo

def process_exception():
    """ 장기간 동안 처리결과를 받지 못하는 pedding task를 처리하는 Process

    20분 동안 처리결과가 안오면, PEDDING_TASK_ZSET 해당 정보를 삭제 후 taskinfo를 pedding.log 파일에 기록

    """
    while True:
        try:
            time.sleep(10)
            conn = get_redis_connection('default')
            pedding_task = conn.zrange(PEDDING_TASK_ZSET, 0, 100, desc=False, withscores=True)
            for task in pedding_task:
                token, timestamp = task
                diff = time.time() - timestamp
                if diff > PEDDING_TASK_AGE:
                    tokeninfo = get_taskinfo(token)
                    tokeninfo_str = json.dumps(tokeninfo)
                    logger_pedding.error(tokeninfo_str)
                    conn.zrem(PEDDING_TASK_ZSET, token)
                    update_status_task(conn,token,STATUS.ABORT.value)
        except Exception as e:
            logger.error("exception occured: %s", e)

exception_processor = multiprocessing.Process(
    name='exception_processor',
    target=process_exception,
)
exception_processor.daemon = True
exception_processor.start()