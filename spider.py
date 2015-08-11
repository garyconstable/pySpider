


import urllib
from urllib.request import urlopen
from urllib.parse import urlparse
import time
from queue import *
import threading
from worker import Worker
import pymysql.cursors





class spider():


    def __init__(self):
        # set vars 
        self.visitedLinks = []

        self.allExtLinks = Queue()
        self.maxThreads = 3
        self.workers = []
        self.running  = True

        #create db connection
        self.initDb()

        #initial Link
        self.startLink()

        #run the spider
        self.run()


    def initDb(self):
        conn = pymysql.connect(host='212.67.214.24', unix_socket='/tmp/mysql.sock', user='gary', passwd='Sarah2004!', db='0090-scraping')
        self.cur = conn.cursor()
        self.cur.execute('USE `0090-scraping`')


    def randomSeed(self):
        self.cur.execute('select url from urls order by rand() limit 1')
        return self.cur.fetchone()['url']


    def startLink(self):
        self.allExtLinks.put({ 
            'url': self.seedUrl()
        })


    def seedUrl(self):
        return 'http://www.reddit.com/'
        return 'http://www.bbc.co.uk'
        return 'http://shopping.indiatimes.com/lifestyle/bed-linen/8-designer-rajasthani-cotton-double-bed-sheets-with-16-pillow-covers/11574/p_B4661019'


    def visitedLinksFromDb(self):
        cur.execute('select url from urls order by url')
        existing = self.cur.fetchall()
        for i in existing:
            self.visitedLinks.append(i['url']) 


    def createWorker(self, allExtLinks, theadNum, cur):
        return Worker(allExtLinks, theadNum, cur)


    def run(self):

        #we have 1 active link
        activeThreads = 1

        ## waiting for output
        print ("Spider: Waiting...")

        #while we are running
        while self.running :

            #if thread count < max - start new thread
            if threading.activeCount() < self.maxThreads:

                #show the the loop is running
                print(' ')
                print(' -------- Ext Links ' + str(self.allExtLinks.qsize()) + ', Threads: ' + str(threading.activeCount()) + ' ----------'  )
                print(' ')

                w = self.createWorker( self.allExtLinks, activeThreads, self.cur )
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
        print ("Spider: Complete...")




if __name__ == '__main__':
    s = spider();
