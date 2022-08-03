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
args = parser.parse_args()

src_dir = args.src_dir
current_time = datetime.now().strftime("%Y%m%d_%H%M%S")


# check source directory exist
def check_srcdir(src_dir):
    if not os.path.isdir(src_dir):
        raise IOError("source directory not found: " + src_dir)

def out(command):
    result = run(command, stdout=PIPE, stderr=PIPE, universal_newlines=True, shell=True)
    return result.stdout

# generate random 6 character
def gen_rand_char():
    char_set = string.ascii_uppercase + string.digits
    return (''.join(random.sample(char_set * 6, 6)))

# start main function
if __name__ == '__main__':

    start_time = datetime.now()
    check_srcdir(src_dir)

    tot = 0
    csv_data = [['full path', 'hash value']]

    for r, d, f in os.walk(src_dir):
        r2 = r.replace('\"', '\\\"')
        command = 'gsutil hash -m -h \"' + r2 + '/*\"'
        output = out(command)
        output = output.split("\n")
        #print(output)
        tot += int(len(output)/2)

        for i in range((int)(len(output)/2)):
            # 2*i 에서는 파일 이름 추출
            output[i*2] = output[i*2].split('for ')[1].strip(':')
            #print(output[i*2], end=',\t')
            # 2*i+1 에서는 해시값 추출
            output[i*2+1] = output[i*2+1].split('\t\t')[1]
            #print(output[i*2+1])

            csv_data.append([output[i*2], output[i*2+1]])
        #print(output)

    file_name = "./res/result" + current_time + gen_rand_char() + ".csv"
    f= open(file_name, "w")
    writer = csv.writer(f)
    writer.writerows(csv_data)
    f.close()

    end_time = datetime.now()
    print(tot)
    print('====================================')
    print('Duration: {}'.format(end_time - start_time))
    print('End')
