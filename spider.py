

import urllib
from urllib.request import urlopen
from urllib.parse import urlparse
import time
from queue import *
import threading
from worker import Worker
from sqlworker import sqlWorker
import pymysql





class spider():


    def __init__(self):
        # set vars 
        self.visitedLinks = set()

        self.allExtLinks = Queue()
        self.maxThreads = 10
        self.workers = []
        self.running  = True

        #create db connection
        self.initDb()

        #initial Link
        self.startLink()

        #run the spider
        self.run()


    def initDb(self):
        '''
        create the database connection
        '''
        conn = pymysql.connect(host='', unix_socket='/tmp/mysql.sock', user='', passwd='', db='')
        self.cur = conn.cursor(pymysql.cursors.DictCursor)
        self.cur.execute('USE `0090-scraping`')


    def startLink(self):
        '''
        add the start link to the queue
        '''
        self.allExtLinks.put({ 
            'url': self.seedUrl()
        })


    def seedUrl(self):
        '''
        the start url
        '''
        return 'http://www.reddit.com/'
        return 'http://www.bbc.co.uk'
        return 'http://shopping.indiatimes.com/lifestyle/bed-linen/8-designer-rajasthani-cotton-double-bed-sheets-with-16-pillow-covers/11574/p_B4661019'


    def createWorker(self, allExtLinks, theadNum, cur, visitedLinks):
        '''
        create the workers
        '''
        return Worker(allExtLinks, theadNum, cur, visitedLinks)


    def run(self):
        '''
        run the app
        '''

        #we have 1 active link
        activeThreads = 1

        #the sql worker
        self.pending  = Queue()

        ## waiting for output
        print ("Spider: Waiting...")

        # create the sql woorker thread
        self.sW = sqlWorker( self.pending, self.cur )
        self.sW.start()


        #while we are running
        while self.running :

            #show the the loop is running
            print(' ')
            print(' -------- Ext Links ' + str(self.allExtLinks.qsize()) + ', Threads: ' + str(threading.activeCount()) + ' ----------'  )
            print(' ')

            #if thread count < max - start new thread
            if threading.activeCount() < self.maxThreads:

                w = self.createWorker( self.allExtLinks, activeThreads, self.cur, self.visitedLinks )
                activeThreads = activeThreads + 1
                self.workers.append(w)
                w.start()
                    
            #end the dead workers
            for w in self.workers:

                #if the worker is still running
                if( w.isAlive() == True):

                    #get the workers visited links
                    for i in w.getVisitedLinks():
                        self.visitedLinks.add(i)

                    #add all of the visited linsk to the worker thread
                    w.setVisitedLinks(self.visitedLinks)

                    #append the waiting data
                    for i in w.getUrlDetails():
                        self.pending.put(i)

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


        self.sW.join()


        ## waiting for output
        print ("Spider: Complete...")




if __name__ == '__main__':
    s = spider();
