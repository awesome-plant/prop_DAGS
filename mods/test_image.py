#used to remote into images and look around
import argparse #add flags here
import time
import os

def wait_time(sleep_time):
    print("start")
    time.sleep(sleep_time)
    print('fin')

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-mod') #module
    parser.add_argument('-sl') #sql limit
    args = vars(parser.parse_args())
    if args['mod'] =='wait_time': #args['mod'] =='openproxy':
        print("creating image to remote into ")
        wait_time(args['sl'])