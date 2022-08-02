## treating arguments
import argparse
import os
import random
import string
from datetime import datetime
from subprocess import PIPE, run
import csv


parser = argparse.ArgumentParser()

# --src_dir ./src
parser.add_argument('--src_dir', help='source directory e) /data/dir1/', action='store', required=True)
parser.add_argument('--tar_dir', action='store', default='./res')
parser.add_argument('--bucket_name', action='store', required=True)
args = parser.parse_args()

src_dir = args.src_dir
tar_dir = args.tar_dir
bucket_name = args.bucket_name
current_time = datetime.now().strftime("%Y%m%d_%H%M%S")


# check source directory exist
def check_srcdir(src_dir):
    if not os.path.isdir(src_dir):
        raise IOError("source directory not found: " + src_dir)

def check_tar(res):
    if not os.path.isdir(res):
        command = "mkdir " + res
        os.system(command)

# generate random 6 character
def gen_rand_char():
    char_set = string.ascii_uppercase + string.digits
    return (''.join(random.sample(char_set * 6, 6)))

def out(command):
    result = run(command, stdout=PIPE, stderr=PIPE, universal_newlines=True, shell=True)
    return result.stdout

# start main function
if __name__ == '__main__':

    start_time = datetime.now()

    src_dirs = open(src_dir).readlines()

    csv_data = [['full path', 'hash value']]
    dir_list = []
    check_tar(tar_dir)

    if tar_dir[-1] == '/':
        tar_dir = tar_dir[:-1]

    for x in range(len(src_dirs)):
        src_dirs[x]=src_dirs[x].strip('\n')
        check_srcdir(src_dirs[x])
        for r, d, f in os.walk(src_dirs[x]):
            if(len(f)>0):
                command = "md5sum \"" + r.replace('\\', '\\\\').replace('\"', '\\\"') + "\"/*"
                dir_list.append([r])

                output = out(command)
                output = output.replace("\\\\", "\\").split("\n")
                for i in range(len(output)):
                    output[i] = output[i].split("  ")
                    if len(output[i]) == 2:
                        csv_data.append(output[i])

    file_name = tar_dir + "/result-" + current_time + gen_rand_char() + ".csv"
    f = open(file_name, "w")
    writer = csv.writer(f)
    writer.writerows(csv_data)
    f.close()

    gsutil = tar_dir + "/dir_list-" + current_time + gen_rand_char() + ".csv"
    f = open(gsutil, "w")
    writer = csv.writer(f)
    for dir in dir_list:
        writer.writerow(dir)
    f.close()

    command = "gsutil cp " + gsutil + " gs://" + bucket_name
    command2 = "gsutil cp " + file_name + " gs://" + bucket_name
    print(command)
    print(command2)
    #os.system(command)
    #os.system(command2)

    end_time = datetime.now()
    print('====================================')
    print('Duration: {}'.format(end_time - start_time))
    print('End')
