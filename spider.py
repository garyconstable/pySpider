import time
from queue import *
import threading
from worker import Worker
import MySQLdb


class Spider:

    def __init__(self):

        self.pending = Queue()
        self.db_engine = MySQLdb.connect(
            host='127.0.0.1',
            user='root',
            passwd='root',
            database='pySpider'
        )

        # set vars 
        self.allExtLinks = Queue()
        self.maxThreads = 3
        self.workers = []
        self.running = True
        self.horizon = []
        self.lock = threading.Semaphore(1)

        # initial Links
        self.loadhorizon()
        if int(self.allExtLinks.qsize()) < 1:
            self.startlink()

        # run the spider
        self.run()

    def startlink(self):
        """
        add the start link to the queue
        """
        self.allExtLinks.put({ 
            'url': 'http://www.reddit.com/'
        })

    def createworker(self, allExtLinks, theadNum, lock):
        """
        create the workers
        :param allExtLinks:
        :param theadNum:
        :param lock:
        :return:
        """
        return Worker(allExtLinks, theadNum, lock, self.db_engine)

    def loadhorizon(self):
        """
        load the horizon list - links not yet visited
        """
        cursor = self.db_engine.cursor(MySQLdb.cursors.DictCursor)
        cursor.execute('select url from pending  order by id asc limit 100')
        rows = cursor.fetchall()
        for row in rows:
            self.allExtLinks.put({ 
                'url': row['url']
            })

    def run(self):
        """
        run the app
        """
        # we have 1 active link
        activethreads = 1

        # the sql worker

        # while we are running
        while self.running:

            # show the the loop is running
            '''
            print(' ')
            print(' -------- Horizon {0}, Threads: {1} ----------'.format(str(self.allExtLinks.qsize()),
                                                                       str(threading.activeCount())))
            print(' ')
            '''

            # if thread count < max - start new thread
            if threading.activeCount() < self.maxThreads:

                w = self.createworker(self.allExtLinks, activethreads, self.lock)
                activethreads = activethreads + 1
                self.workers.append(w)
                w.start()
                    
            # end the dead workers
            for w in self.workers:

                # join the dead threads and count
                if not w.isAlive():
                    w.join()
                    activethreads = activethreads - 1
                    self.workers.remove(w)

            # # sleep per loop
            # time.sleep(0.25)

            if self.allExtLinks.empty():
                self.running = False

        # join active threads - to end the app
        while threading.activeCount() > 1:
            for w in self.workers:
                w.join()             

        # waiting for output
        '''        
        print(
            ' -------- Complete: horizon {0}, Threads: {1}, saved: {2} ----------'.format(str(self.allExtLinks.qsize()),
                                                                                          str(threading.activeCount()),
                                                                                          str(self.written)))
        '''


if __name__ == '__main__':
    s = Spider()
