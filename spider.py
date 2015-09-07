

import urllib
from urllib.request import urlopen
from urllib.parse import urlparse
import time
from queue import *
import threading
from worker import Worker
#from conn import *
#import pymysql.cursors
import pymysql
import sqlalchemy
from sqlalchemy import create_engine


class spider():


    def __init__(self):

        #db connect
        #self.db_engine = create_engine('mysql+pymysql://gary:Sarah2004!@212.67.214.24/0090-scraping', max_overflow=10, pool_size=5, pool_recycle=3600)
        self.db_engine = create_engine('mysql+pymysql://gary:Sarah2004!@212.67.214.24/0090-scraping')

        # set vars 
        self.allExtLinks = Queue()
        self.maxThreads = 5
        self.workers = []
        self.running  = True
        self.horizon = []
        self.lock = threading.Semaphore(1)

        #initial Links
        self.loadHorizon()
        if int(self.allExtLinks.qsize()) < 1:
            self.startLink()

        #run the spider
        self.run()


    def startLink(self):
        '''
        add the start link to the queue
        '''
        self.allExtLinks.put({ 
            'url': 'http://www.reddit.com/'
        })


    def createWorker(self, allExtLinks, theadNum, lock):
        '''
        create the workers
        '''
        return Worker(allExtLinks, theadNum, lock, self.db_engine)


    def loadHorizon(self):
        '''
        load the horizon list - links not yet visited 
        '''
        print(' ---  Begin load pending ---')

        for row in self.db_engine.execute('select url from pending'):
            self.allExtLinks.put({ 
                'url': row['url']
            })

        print(' ---  End load pending ---')


    def run(self):
        '''
        run the app
        '''

        #we have 1 active link
        activeThreads = 1

        #the sql worker
        self.pending  = Queue()

        #while we are running
        while self.running :

            #show the the loop is running
            print(' ')
            print(' -------- Horizon ' + str(self.allExtLinks.qsize()) + ', Threads: ' + str(threading.activeCount()) +' ----------'  )
            print(' ')

            #if thread count < max - start new thread
            if threading.activeCount() < self.maxThreads:

                w = self.createWorker( self.allExtLinks, activeThreads, self.lock )
                activeThreads = activeThreads + 1
                self.workers.append(w)
                w.start()
                    
            #end the dead workers
            for w in self.workers:

                # join the dead threads and count
                if( w.isAlive() == False):
                    w.join()
                    activeThreads = activeThreads - 1
                    self.workers.remove(w)

            #sleep 1 second per loop
            time.sleep(1)    

            #end the loop if no more
            if( self.allExtLinks.empty() == True ):
                self.running = False

        #join active threads - to end the app
        while threading.activeCount()>1:            
            for w in self.workers:
                w.join()             

        ## waiting for output
        print(' -------- Complete: horizon ' + str(self.allExtLinks.qsize()) + ', Threads: ' + str(threading.activeCount()) + ', saved: ' + str(self.written) +' ----------'  )

if __name__ == '__main__':
    s = spider();

    
