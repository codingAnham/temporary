## treating arguments
import argparse
import os
from datetime import datetime
import multiprocessing
from subprocess import PIPE, run


parser = argparse.ArgumentParser()
parser.add_argument('--src_dir', help='source directory e) /data/dir1/', action='store', required=True)
parser.add_argument('--max_process', help='NUM e) 5', action='store', default=5, type=int)
args = parser.parse_args()

prefix_list = args.src_dir  ## Don't forget to add last slash '/'
max_process = args.max_process
ans = []

quit_flag = 'DONE'

current_time = datetime.now().strftime("%Y%m%d_%H%M%S")

def multi_command(ans):
    while True:
        root = ans.get()
        if root == quit_flag:
            break
        # os.system(command)
    return

def run_multi_command(ans):
    procs=[]
    for i in range(max_process):
        proc = multiprocessing.Process(target=multi_command, args=(ans, ))
        procs.append(proc)
        proc.start()
    finish_q(ans, procs)

def finish_q(q, p_list):
    for j in range(max_process):
        q.put(quit_flag)
    for pi in p_list:
        pi.join()

# start main function
if __name__ == '__main__':

    start_time = datetime.now()
    src_dirs = open(prefix_list).readlines()
    ans = multiprocessing.Manager().Queue()

    for x in range(len(src_dirs)):
        ans.put(src_dirs[x].strip('\n'))

    run_multi_command(ans)

    end_time = datetime.now()
    print('====================================')
    print('Duration: {}'.format(end_time - start_time))
    print('End')
