

import sys, traceback
from threading import Thread
import re
from queue import *
import time
import csv


class horizonWorker(Thread):


    def __init__( self, queue, horizon, lock ):
        '''
        init with the queue and set the logger - this worker is a thread extended class
        '''
        Thread.__init__(self)
        self.queue = queue
        self.running = True
        self.horizon = horizon
        self.lock = lock


    def join(self, timeout=None):
        '''
        when the thread joins send the loop end signal
        '''
        self.running = False
        super(horizonWorker, self).join(timeout)


    def setRunning(self, running):
        '''
        change the running state of the thread
        '''
        self.running = running


    def run(self):
        '''
        thread run ...
        '''
        
        #get the item from the queue
        item = self.queue.get()

        #while there is an item in the current queue
        while self.running == True:                 

            try:

                self.lock.acquire()

                print(' -------- Horizon worker begin dump: ' + str(self.queue.qsize()) +' ---------- ')

                with open('csv/horizon.csv', 'a', newline='') as csvfile:
                    writer = csv.writer(csvfile, delimiter=' ',quotechar='|', quoting=csv.QUOTE_MINIMAL)
                    for elem in list(self.queue.queue):
                        if(elem['url'] not in self.horizon):
                            self.horizon.append(elem['url'])
                            writer.writerow([elem['url']])    

                self.lock.release()

                time.sleep(30)

            except Exception as e:

                print(' ')
                print(' -------- Horizon Worker exception: -------- ');
                print(e)
                print(' ')

                print("-"*60)
                traceback.print_exc(file=sys.stdout)
                print("-"*60)

                continue

        self.queue.task_done() 

                

        
