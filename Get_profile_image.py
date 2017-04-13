#!/usr/bin/env python
# -*- coding: utf-8 -*-

import base64
import json
import os
import random
import re
import sys
import threading
import time
import urllib
import urllib2

import MySQLdb

LOCAL_DIR = 'twitter_user_profile_image_1'
Table = 'Twitter_User_Details_Twitter_170401_run'
THREAD_COUNT = 50
schedule = 0
# profile_image_url_local
HOST, USER, PASSWD, DB, PORT = '****', '****', '****', '****', ****
#HOST,USER,PASSWD,DB,PORT = '1.192.90.168','jiaowei','jiaowei_123','CollectTwitter',33306
secect_sql = 'select user_id,profile_image_url_https,flag from ' + \
    Table + ' where flag = 1'
update_sql = 'update ' + Table + ' set flag = 2 where user_id = %s'
sql_count = 'select count(*) from ' + Table + '  where flag = 1'

# restart program


def restart_program():
    python = sys.executable
    os.execl(python, python, * sys.argv)

# connect to mysql


def ConnectDB():
    connect, cursor = None, None
    restart_flag = 0
    while True:
        try:
            connect = MySQLdb.connect(host=HOST, user=USER,
                                      passwd=PASSWD, db=DB, port=PORT, charset='utf8')
            cursor = connect.cursor()
            break
        except Exception, e:
            time.sleep(5)
            restart_flag += 1
            print 'ERROR:ConnectDB() ', e
            if restart_flag >= 30:
                print 'ConnectDB failed 30 times then reatart the program!'
                restart_program()
    return connect, cursor


class GetPictures(threading.Thread):
    def __init__(self, lock, threadID, tasklist, Total_TaskNum):
        super(GetPictures, self).__init__()
        self.lock = lock
        self.threadID = threadID
        self.tasklist = tasklist
        self.Total_TaskNum = Total_TaskNum

    def run(self):
        global schedule
        self.lock.acquire()
        print 'The Thread tasklist number:', len(self.tasklist)
        self.lock.release()
        download_profile_image(self.tasklist, self.threadID)


def download_profile_image(url_list, id):
    count = 0
    global LOCAL_DIR
    #update_list = []
    for (user_id, url_https, flag) in url_list:
        failed_flag = 0
        continue_flag = 0
        last_name = url_https[url_https.rindex('.') + 1:]
        if len(last_name)>10:
            last_name = 'jpg'
        local_dir = LOCAL_DIR
        #local_dir = local_dir + '/' + user_id + '.' + last_name
        if os.path.exists(local_dir):
            # print 'the dir is already exists,then continue'
            pass
        else:
            print 'dir is not exits,then create the dir: ', local_dir
            os.makedirs(local_dir)
        # print last_name
        file_name = local_dir + '/' + user_id + '.' + last_name
        if os.path.exists(file_name):
            print 'the picture is already exists,then continue,and check flag is 2?'
            if flag != 2:
                try:
                    connect, cursor = ConnectDB()
                    cursor.execute(update_sql % user_id)
                    connect.commit()
                    connect.close()
                    time.sleep(2)
                except Exception, e:
                    time.sleep(2)
                    print 'ERROR:update is failed'
                    continue
            continue
        while 1:
            try:
                time.sleep(2)
                request = urllib2.Request(url_https)
                response = urllib2.urlopen(request, timeout=5)
                pic = response.read()
                break
            except Exception, e:
                failed_flag += 1
                print 'ERROR:url get failed,then pass the url', e
                if failed_flag >= 10:
                    print 'get image failed 10 times,then continue'
                    fo = open('get_url_error.txt', 'a+')
                    fo.write('***user_id*** = ' + user_id +
                             ' &&&url_https&&& = ' + url_https + '\n')
                    fo.close()
                    continue_flag = 1
                    break
        if continue_flag:
            continue
        try:
            fo = open(file_name, 'wb+')
            fo.write(pic)
            fo.close()
        except Exception, e:
            print "ERROR:write pic failed,then continue:", e
            continue
        count += 1
        print 'threadID = %s -----当前进度：%s/%s' % (str(id), count, len(url_list))
        try:
            connect, cursor = ConnectDB()
            cursor.execute(update_sql % user_id)
            connect.commit()
            connect.close()
            #update_list = []
            time.sleep(2)
        except Exception, e:
            time.sleep(2)
            print 'ERROR:update is failed'
            continue
    print 'download_profile_image is already over!'


def Thread_Handle(tasklist, Total_TaskNum):
    global THREAD_COUNT
    lock = threading.Lock()
    Threadpool = []
    every_thread_num = len(tasklist) / THREAD_COUNT
    if every_thread_num == 0:
        THREAD_COUNT = len(tasklist)
        every_thread_num = 1

    for i in range(0, THREAD_COUNT):
        if i != THREAD_COUNT - 1:
            source_list = tasklist[i *
                                   every_thread_num:(i + 1) * every_thread_num]
            works = GetPictures(lock, i, source_list, Total_TaskNum)
            works.start()
            Threadpool.append(works)
        else:
            source_list = tasklist[i * every_thread_num:]
            works = GetPictures(lock, i, source_list, Total_TaskNum)
            works.start()
            Threadpool.append(works)

    for works in Threadpool:
        works.join()


def main():
    connect, cursor = ConnectDB()
    cursor.execute(sql_count)
    task_num = cursor.fetchone()
    connect.close()
    if task_num[0] == 0:
        print 'Warning:There is no work to do !'
    else:
        Total_TaskNum = int(task_num[0])
        print 'Total task length is : ', Total_TaskNum
        while True:
            try:
                connect, cursor = ConnectDB()
                if cursor.execute(secect_sql):
                    taskList = cursor.fetchall()
                    connect.close()
                    Thread_Handle(taskList, Total_TaskNum)
                    break
                else:
                    break
            except Exception, e:
                print 'ERROR: main() ', e
                time.sleep(5)


if __name__ == '__main__':

    print "The Program start time:", time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    start = time.time()
    main()
    connect, cursor = ConnectDB()
    cursor.execute(sql_count)
    task_num = cursor.fetchone()
    connect.close()
    if task_num[0] == 0:
        print "The Program end time:", time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()), "[%s]" % (time.time() - start)
        print 'Game Over!'
    else:
        print 'The work is not over yet,then restart the program!!!!!!!'
        time.sleep(3)
        restart_program()
