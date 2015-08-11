

import sys, traceback
from threading import Thread
import urllib
from urllib.request import urlopen
from urllib.parse import urlparse
from bs4 import BeautifulSoup
import logging
import re
from queue import *
import pymysql
import time


'''
logging config
'''
logging.basicConfig(level=logging.DEBUG)


class sqlWorker(Thread):


    def __init__( self, queue, cur ):
        '''
        init with the queue and set the logger - this worker is a thread extended class
        '''
        Thread.__init__(self)
        self.cursor = cur      
        self.queue = queue
        self.running = True


    def join(self, timeout=None):
        '''
        when the thread joins send the loop end signal
        '''
        self.running = False
        super(sqlWorker, self).join(timeout)


    def saveLink(self, url, title, description):
        '''
        see if the url exists
        '''
        self.cursor.execute( 'select * from urls where url = %s', (url) )
        r = self.cursor.fetchone()
        url_id = 0

        if r == None:
            self.cursor.execute('insert into urls (url) values (%s)', url)
            self.cursor.execute('select LAST_INSERT_ID()')
            url_id = self.cursor.fetchone()[0]
        else:
            url_id = r['urls_id']


        '''
        see if the title exists
        '''
        self.cursor.execute('select * from phrazes where phraze = %s', (title))
        r = self.cursor.fetchone()

        title_id = 0

        if r == None:
            self.cursor.execute('insert into phrazes (phraze) values (%s)', title)
            self.cursor.execute('select LAST_INSERT_ID()')
            title_id = self.cursor.fetchone()[0]
        else:
            title_id = r['phrazes_id']


        '''
        add title to piviot
        '''
        self.cursor.execute('select * from url_phraze_pivot where urls_id = %s and phrazes_id = %s', (url_id, title_id))
        r = self.cursor.fetchone()
        if r == None:
            self.cursor.execute('insert into url_phraze_pivot (urls_id, phrazes_id, occurrences) values (%s, %s, 0)', (url_id, title_id))
        else:
            self.cursor.execute('update url_phraze_pivot set occurrences = occurrences + 1 where urls_id = %s and phrazes_id = %s ', (url_id, title_id))


        '''
        see if the description exists
        '''
        self.cursor.execute( 'select * from phrazes where phraze = %s', (description) )
        r = self.cursor.fetchone()

        description_id = 0

        if r == None:
            self.cursor.execute('insert into phrazes (phraze) values (%s)', description)
            self.cursor.execute('select LAST_INSERT_ID()')
            description_id = self.cursor.fetchone()[0]
        else:
            description_id = r['phrazes_id']


        '''
        add title to piviot
        '''
        self.cursor.execute('select * from url_phraze_pivot where urls_id = %s and phrazes_id = %s', (url_id, description_id))
        r = self.cursor.fetchone()
        if r == None:
            self.cursor.execute('insert into url_phraze_pivot (urls_id, phrazes_id, occurrences) values (%s, %s, 0)', (url_id, description_id))
        else:
            self.cursor.execute('update url_phraze_pivot set occurrences = occurrences +1 where urls_id = %s and phrazes_id = %s ', (url_id, description_id))


        print(' ')
        print(' -------- save link -------- ')
        print(url, title, description)
        print(' ')  


    def run(self):
        '''
        thread run, sace url
        '''
        
        #get the item from the queue
        item = self.queue.get()

        #while there is an item in the current queue
        while self.running == True:                 

            try:

                if item != None:

                    #get the parts
                    url = item['url']
                    title = item['title']
                    description = item['description']

                    #save the parts
                    self.saveLink(url, title, description)


                time.sleep(2)

                #set the next item for the while loop
                item = self.queue.get()                

            except Empty as e:
                self.running = False
                continue

            except Exception as e:

                print(' ')
                print(' -------- Sql Worker exception ' + self.threadNum + ': -------- ');
                print(e)
                print(' ')

                print("-"*60)
                traceback.print_exc(file=sys.stdout)
                print("-"*60)

                continue

        self.queue.task_done() 

                

        
