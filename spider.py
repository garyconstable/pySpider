

import urllib
from urllib.request import urlopen
from urllib.parse import urlparse
import time
from queue import *
import threading
from worker import Worker
import csv


class spider():


    def __init__(self):


        # set vars 
        self.visitedLinks = set()
        self.allExtLinks = Queue()
        self.maxThreads = 10
        self.workers = []
        self.writtenLinks = []
        self.running  = True
        self.horizon = []
        self.lock = threading.Semaphore(1)

        #initial Links
        self.loadHorizon()
        if int(self.allExtLinks.qsize()) < 1:
            self.startLink()

        #already visited links
        self.loadVisited()

        #run the spider
        self.run()


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


    def createWorker(self, allExtLinks, theadNum, visitedLinks, lock):
        '''
        create the workers
        '''
        return Worker(allExtLinks, theadNum, visitedLinks, lock)


    def loadHorizon(self):
        '''
        load the horizon list 
        '''
        with open('csv/horizon.csv', newline='') as csvfile:
            reader = csv.reader(csvfile, delimiter=' ', quotechar='|')
            for row in reader: 
                self.allExtLinks.put({ 
                    'url': row[0]
                })


    def saveHorizon(self):
        '''
        save the queue
        '''
        print(' --- run timer ---')
        timer = threading.Timer(60.0, self.saveHorizon)
        timer.start()

        self.lock.acquire()
        with open('csv/horizon.csv', 'a', newline='') as csvfile:
            writer = csv.writer(csvfile, delimiter=' ',quotechar='|', quoting=csv.QUOTE_MINIMAL)
            for elem in list(self.allExtLinks.queue):
                if(elem['url'] not in self.horizon):
                    self.horizon.append(elem['url'])
                    writer.writerow([elem['url']])    
        self.lock.release()

        #timer.cancel()
        #timer.join()


    def loadVisited(self):
        '''
        load the complete list of visited links
        '''
        with open('csv/visited-links.csv', newline='') as csvfile:
            reader = csv.reader(csvfile, delimiter=' ', quotechar='|')
            for row in reader: 
                self.visitedLinks.add(row[0])


    def saveVisited(self):
        '''
        save the visited
        '''
        self.lock.acquire()            
        #links that we have visited
        with open('csv/pending-links.csv', 'a', newline='') as csvfile:
            writer = csv.writer(csvfile, delimiter=' ',quotechar='|', quoting=csv.QUOTE_MINIMAL)
            for url in self.visitedLinks:
                if url not in self.writtenLinks:
                    writer.writerow([url])
                    self.written = self.written + 1
                    self.writtenLinks.append(url)
        self.lock.release()


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

        self.written = 0

        #while we are running
        while self.running :

            #show the the loop is running
            print(' ')
            print(' -------- Horizon ' + str(self.allExtLinks.qsize()) + ', Threads: ' + str(threading.activeCount()) + ', saved: ' + str(self.written) +' ----------'  )
            print(' ')

            #if thread count < max - start new thread
            if threading.activeCount() < self.maxThreads:

                w = self.createWorker( self.allExtLinks, activeThreads, self.visitedLinks, self.lock )
                activeThreads = activeThreads + 1
                self.workers.append(w)
                w.start()
                    
            #end the dead workers
            for w in self.workers:

                #if the worker is still running
                if( w.isAlive() == True):
                
                    #get the workers visited links
                    self.visitedLinks.union(w.getVisitedLinks())

                    #add all of the visited linsk to the worker thread
                    w.setVisitedLinks(self.visitedLinks)

                # join the dead threads and count
                if( w.isAlive() == False):
                    w.join()
                    activeThreads = activeThreads - 1
                    self.workers.remove(w)

            #save the visited links
            self.saveVisited()

            #task run every few mins
            self.saveHorizon()

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
