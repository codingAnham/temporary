
## treating arguments
import argparse
import logging
import multiprocessing
import os
import random
import string
import sys
from datetime import datetime
import io
import tarfile
import traceback
import socket
from pathlib import Path
from subprocess import PIPE, run


parser = argparse.ArgumentParser()

# --src_dir ./src ./src2 ./src3 --target_file_prefix ./tar --max_cnt 10
parser.add_argument('--src_dir', help='source directory e) /data/dir1/', action='store', required=True)
parser.add_argument('--max_process', help='NUM e) 5', action='store', default=5, type=int)
parser.add_argument('--compression', help='specify gz to enable', action='store', default='')
parser.add_argument('--target_file_prefix', help='prefix of the target file we are creating into the snowball', action='store', default='')
parser.add_argument('--symlinkdir', help='indicate to follow syblic link or not, default is no', action='store', default='no')
""" 변수 추가 """
parser.add_argument('--max_cnt', help='NUM e) 5', action='store', default=5, type=int)
args = parser.parse_args()

prefix_list = args.src_dir  ## Don't forget to add last slash '/'
##Common Variables
max_process = args.max_process
compression = args.compression  # default for no compression, "gz" to enable
target_file_prefix = args.target_file_prefix
if args.symlinkdir == 'yes':
    symlinkdir = True
else:
    symlinkdir = False
log_level = logging.INFO  ## DEBUG, INFO, WARNING, ERROR
""" 변수 추가 """
max_cnt = args.max_cnt

current_time = datetime.now().strftime("%Y%m%d_%H%M%S")
# CMD variables
cmd = 'upload_sbe'  ## supported_cmd: 'download|del_obj_version|restore_obj_version'
# create log directory
try:
    os.makedirs('log')
except:
    pass

host_name = socket.gethostname()

errorlog_file = "log/error-" + host_name + "-%s.log" % current_time
successlog_file = "log/success-" + host_name  + "-%s.log" % current_time
filelist_file = "log/filelist-" + host_name  + "-%s.log" % current_time


quit_flag = 'DONE'
# End of Variables

if os.name == 'posix':
    multiprocessing.set_start_method("fork")


# defining function
## setup logger
def setup_logger(logger_name, log_file, level=logging.INFO, sHandler=False):
    l = logging.getLogger(logger_name)
    formatter = logging.Formatter('%(message)s')
    fileHandler = logging.FileHandler(log_file, mode='a', encoding='utf-8')
    fileHandler.setFormatter(formatter)
    streamHandler = logging.StreamHandler()
    streamHandler.setFormatter(formatter)
    l.setLevel(level)
    l.addHandler(fileHandler)
    if sHandler:
        l.addHandler(streamHandler)
    else:
        pass


## define logger
setup_logger('error', errorlog_file, level=log_level, sHandler=True)
setup_logger('success', successlog_file, level=log_level, sHandler=True)
setup_logger('filelist', filelist_file, level=log_level, sHandler=False)
error_log = logging.getLogger('error')
success_log = logging.getLogger('success')
filelist_log = logging.getLogger('filelist')


# check source directory exist
def check_srcdir(src_dir):
    if not os.path.isdir(src_dir):
        raise IOError("source directory not found: " + src_dir)


def out(command):
    result = run(command, stdout=PIPE, stderr=PIPE, universal_newlines=True, shell=True)
    return result.stdout


def run_multip(max_process, exec_func, q, tar_dir, lock):
    p_list = []
    for i in range(max_process):
        p = multiprocessing.Process(target=exec_func, args=(q, tar_dir, lock,))
        p_list.append(p)
        p.daemon = True
        p.start()
    return p_list


# generate random 6 character
def gen_rand_char():
    char_set = string.ascii_uppercase + string.digits
    return (''.join(random.sample(char_set * 6, 6)))


## code from snowball_uploader
def make_tar_file(tar_name, tar_dir, org_files_list, l):
    if len(org_files_list) == 0:
        return
    delimeter = ' , '
    recv_buf = io.BytesIO()
    collected_files_no = 0
    success_log.info('%s is archiving', tar_name)
    # with tarfile.open(fileobj=recv_buf, mode='w:'+compression) as tar:

    # 압축파일 저장 폴더가 없는 경우 생성
    if not os.path.exists(tar_dir):
        os.makedirs(tar_dir)

    command = "7z a -spf -mx=0 " + tar_name
    # command = "7z a " + tar_name

    for full_path, file_name in org_files_list:
        try:
            # full_path = full_path.replace('\\','/').replace('//','/\\')
            l.acquire()
            filelist_log.info(tar_name + delimeter + file_name + delimeter + full_path)
            l.release()
            full_path = full_path.replace('\\', '\\\\').replace('\"', '\\\"')
            command += " \"" + full_path + "\""
            collected_files_no += 1
        except:
            error_log.info("%s is ignored" % file_name)

    # print(command)
    # os.system(command)
    output = out(command)
    # print(output)
    error_log.info(output)

    recv_buf.seek(0)
    success_log.info('%s uploading', tar_name)

    return collected_files_no


def upload_file(q, tar_dir, l):
    # global target_file_prefix
    while True:
        mp_data = q.get()
        org_files_list = mp_data
        randchar = str(gen_rand_char())
        if compression == '':
            tar_name = ('%s/GCS-%s-%s.7z' % (tar_dir, current_time, randchar))
        elif compression == 'gz':
            tar_name = ('%s/GCS-%s-%s.tgz' % (tar_dir, current_time, randchar))
        l.acquire()
        success_log.debug('receving mp_data size: %s' % len(org_files_list))
        success_log.debug('receving mp_data: %s' % org_files_list)
        l.release()
        if mp_data == quit_flag:
            break
        try:
            make_tar_file(tar_name, tar_dir, org_files_list, l)
            # print('%s is uploaded' % tar_name)
        except Exception as e:
            l.acquire()
            error_log.info('exception error: %s uploading failed' % tar_name)
            error_log.info(e)
            l.release()
            traceback.print_exc()
        # return 0 ## for the dubug, it will pause with error


# get files to upload
def upload_get_files(sub_prefix, q, l):
    num_obj = 0
    sum_size = 0
    """ 변수 추가 """
    cnt = 0
    org_files_list = []
    # get all files from given directory
    for r, d, f in os.walk(sub_prefix, followlinks=symlinkdir):
        for file in f:
            file_name1 = os.path.join(r, file)
            if (Path(file_name1).is_symlink()) and (not Path(file_name1).exists()):
                print("Warning: " + file + " is broken symlink file, it will be ignored")
            else:
                try:
                    full_path = os.path.join(r, file)
                    """변수 추가"""
                    file_name = file  # r.replace('\\', '/')+"/"+file
                    file_info = (full_path, file_name)  # file_info에 full_path 추가
                    org_files_list.append(file_info)
                    """ 변수 추가 """
                    cnt += 1
                    if max_cnt <= cnt:
                        cnt = 0
                        mp_data = org_files_list
                        org_files_list = []
                        try:
                            q.put(mp_data)
                            l.acquire()
                            success_log.debug('0, sending mp_data size: %s' % len(mp_data))
                            success_log.debug('0, sending mp_data: %s' % mp_data)
                            l.release()
                        except Exception as e:
                            l.acquire()
                            error_log.info('exception error: putting %s into queue is failed' % file_name)
                            error_log.info(e)
                            l.release()
                    num_obj += 1
                except Exception as e:
                    l.acquire()
                    error_log.info('exception error: getting %s file info is failed' % file_name)
                    error_log.info(e)
                    l.release()
    try:
        # put remained files into queue
        mp_data = org_files_list
        q.put(mp_data)
        l.acquire()
        success_log.debug('1, sending mp_data size: %s' % len(mp_data))
        success_log.debug('1, sending mp_data: %s' % mp_data)
        l.release()
    except Exception as e:
        l.acquire()
        error_log.info('exception error: putting %s into queue is failed' % file_name)
        error_log.info(e)
        l.release()
    return num_obj


def finishq(q, p_list):
    for j in range(max_process):
        q.put(quit_flag)
    for pi in p_list:
        pi.join()


def upload_file_multi(src_dir, tar_dir, lock):
    success_log.info('%s directory is uploading' % src_dir)
    p_list = run_multip(max_process, upload_file, q, tar_dir, lock)
    # get object list and ingest to processes
    num_obj = upload_get_files(src_dir, q, lock)
    # sending quit_flag and join processes
    finishq(q, p_list)
    success_log.info('%s directory is uploaded' % src_dir)
    return num_obj


# start main function
if __name__ == '__main__':

    # define simple queue
    # q = multiprocessing.Queue()
    q = multiprocessing.Manager().Queue()
    start_time = datetime.now()
    success_log.info("starting script..." + str(start_time))
    src_dirs = open(prefix_list).readlines()
    tar_dirs = open(target_file_prefix).readlines()

    if len(src_dirs) != len(tar_dirs):
        error_log.info(':: length of src_dirs and tar_dirs should be same')
        sys.exit()

    for x in range(len(src_dirs)):
        src_dirs[x]=src_dirs[x].strip('\n')
        tar_dirs[x] = tar_dirs[x].strip('\n')

        check_srcdir(src_dirs[x])

        if cmd == 'upload_sbe':
            lock = multiprocessing.Lock()
            total_files = upload_file_multi(src_dirs[x], tar_dirs[x], lock)

    end_time = datetime.now()
    print('====================================')
    print('Duration: {}'.format(end_time - start_time))
    print('Total File numbers: %d' % total_files)  # kyongki
    print('End')
