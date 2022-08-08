## treating arguments
import argparse
import os
import random
import string
from datetime import datetime
from subprocess import PIPE, run
import csv
import multiprocessing
import time
import signal
import errno

parser = argparse.ArgumentParser()

# --src_dir ./src
parser.add_argument('--src_dir', help='source directory e) /data/dir1/', action='store', required=True)
parser.add_argument('--tar_dir', action='store', default='./res')
parser.add_argument('--bucket_name', action='store', required=True)
parser.add_argument('--max_process', help='NUM e) 5', action='store', default=5, type=int)
args = parser.parse_args()

src_dir = args.src_dir
tar_dir = args.tar_dir
bucket_name = args.bucket_name
max_process = args.max_process
current_time = datetime.now().strftime("%Y%m%d_%H%M%S")

quit_flag = 'DONE'


# check source directory exist
def check_srcdir(src_dir):
    if not os.path.isdir(src_dir):
        raise IOError("source directory not found: " + src_dir)

def check_tar(res):
    if not os.path.isdir(res):
        command = "mkdir " + res
        os.system(command)

    if res[-1] == '/':
        res = res[:-1]
    return res

# generate random 6 character
def gen_rand_char():
    char_set = string.ascii_uppercase + string.digits
    return (''.join(random.sample(char_set * 6, 6)))

def out(command):
    result = run(command, stdout=PIPE, stderr=PIPE, universal_newlines=True, shell=True)
    return result.stdout

def dump(q):
    res = []
    q.put('STOP')
    for i in iter(q.get, 'STOP'):
        res.append(i)
    while not q.empty():
        q.get
    for r in res:
        q.put(r)
    return res

def make_dir_list(src_list, dir_list):
    while True:
        src = src_list.get()
        if src==quit_flag:
            #print(quit_flag)
            return
        check_srcdir(src)
        for r,d,f in os.walk(src):
            if len(f)>0:
                r2 = r.replace('\\', '\\\\').replace('\"', '\\\"')
                #print(r2)
                dir_list.put(r2)

def make_hash(dir_list, csv_data):
    while True:
        mp_data = dir_list.get()
        if mp_data == quit_flag:
            #print(quit_flag)
            break
        command = "md5sum \"" + mp_data + "\"/*"
        #print(command)
        output = out(command)
        output = output.replace("\\\\", "\\").split("\n")
        for i in range(len(output)):
            output[i] = output[i].split("  ")
            if len(output[i]) == 2:
                csv_data.append([output[i][1], output[i][0]])

def run_multi_list(src_list, dir_list):
    procs=[]
    for i in range(max_process):
        proc = multiprocessing.Process(target=make_dir_list, args=(src_list,dir_list,))
        procs.append(proc)
        proc.daemon=True
        proc.start()
    finish_q(src_list, procs)

def run_multi_hash(dir_list, csv_data):
    procs=[]
    signal.signal(signal.SIGCHLD, wait_child)
    for i in range(max_process):
        proc = multiprocessing.Process(target=make_hash, args=(dir_list, csv_data,))
        procs.append(proc)
        proc.daemon = True
        proc.start()
    finish_q(dir_list, procs)

def wait_child(signum, frame):
    try:
        while True:
            cpid, status = os.waitpid(-1, os.WNOHANG)
            if cpid==0:
                #print("no child process was immediately available")
                break
            exitcode = status >> 8
            #print("child process {} exit with exitcode {}" .format(cpid, exitcode))
    except OSError as e:
        if e.errno == errno.ECHILD:
            #print("current process has no existing unwaited-for child process")
            pass
        else:
            raise
    #print("handle SIGCHILD end")

def finish_q(q, p_list):
    for j in range(max_process):
        q.put(quit_flag)
    for pi in p_list:
        pi.join()

def make_csv(csv_data):
    file_name = tar_dir + "/result-" + current_time + gen_rand_char() + ".csv"
    f = open(file_name, "w")
    writer = csv.writer(f)
    writer.writerows(csv_data)
    f.close()

    command = "gsutil cp " + file_name + " gs://" + bucket_name
    print(command)
    #os.system(command)

def make_txt(d_list):
    file_name = tar_dir + "/dir_list-" + current_time + gen_rand_char() + ".txt"
    f = open(file_name,"w")
    for dir in d_list:
        f.write(dir.replace('\\\\','\\').replace('\\\"','\"').replace('\\\'','\'')+'\n')
    f.close()

    command = "gsutil cp " + file_name + " gs://" + bucket_name
    print(command)
    # os.system(command)

# start main function
if __name__ == '__main__':
    start_time = datetime.now()
    # 일단 queue 대신에 list로 해서 미리 task 나눠주기 시도해보기
    csv_data = multiprocessing.Manager().list() # csv_data
    src_list = multiprocessing.Manager().Queue() # src_dirs
    dir_list = multiprocessing.Manager().Queue() # dir_list


    csv_data.append(['full path', 'hash value'])
    src_dirs = open(src_dir).readlines()
    for i in range(len(src_dirs)):
        src_list.put(src_dirs[i].strip('\n'))
    tar_dir = check_tar(tar_dir)

    #print("make list in 1sec")
    #time.sleep(1)
    run_multi_list(src_list, dir_list)
    d_list = dump(dir_list)
    make_txt(d_list)
    
    #print("make list finish")
    #print("make hash start")
    #time.sleep(1)
    run_multi_hash(dir_list, csv_data)
    make_csv(csv_data)
    #print("make hash finish")

    end_time = datetime.now()
    print('====================================')
    print('Duration: {}'.format(end_time - start_time))
    print('End')
