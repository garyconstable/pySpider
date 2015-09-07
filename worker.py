

import sys, traceback
from threading import Thread
import urllib
from urllib.request import urlopen
from urllib.parse import urlparse
from bs4 import BeautifulSoup
import logging
import re
from queue import *
import time
import pymysql
import sqlalchemy
from sqlalchemy import create_engine


logging.basicConfig(level=logging.DEBUG)


class Worker(Thread):


    def __init__( self, queue, threadNum, lock, engine ):
        '''
        init with the queue and set the logger - this worker is a thread extended class
        '''
        Thread.__init__(self)
        self.threadNum = str(threadNum)    
        self.queue = queue;
        self.running = True
        self.lock = lock
        self.excludedDomains = []
        self.setExcludedDomains()
        self.engine = engine


    def setExcludedDomains(self):
        '''
        domains that we want to exclude from the search
        '''
        self.excludedDomains.append('www.reddit.com')
        self.excludedDomains.append('www.deviantart.com')
        self.excludedDomains.append('comments.deviantart.com')


    def join(self, timeout=None):
        '''
        when the thread joins send the loop end signal
        '''
        self.running = False
        super(Worker, self).join(timeout)      


    def getCurrentDomain(self, page):
        '''
        get the domain we are crawling
        '''
        parsed_uri = urlparse(page)
        return parsed_uri.netloc


    def encodeLink(self, link):
        '''
        attempt to fix encoding issues with links
        '''
        input = bytes(link, "UTF-8")
        link = input.decode("ascii", "ignore")
        return link;


    def fetch(self, uri):
        '''
        url open, check the headers for text/html
        if so return data
        '''
        uri = self.encodeLink(uri)      
        try:
            h = urlopen(uri)
            x =  h.info()
            if x['Content-Type'] != None and 'text/html' in x['Content-Type'].lower():
                return h.read()
            else:
                return None
        except urllib.error.URLError:
            return None


    def getLinks(self, page, url):
        '''
        find all anchor linsk within the page
        add to either array depending on its http(s) status
        '''
        internalLinks = [];
        externalLinks = [];
        currentDomain = self.getCurrentDomain(url)
        self.urlparse = urlparse(url)

        for link in page.findAll('a', href=True):

            if link.has_attr('href'):
                
                if( link['href'].startswith('#') == False and link['href'] != '/' ):
     
                    #internal link - rel link
                    if currentDomain not in link['href'] and link['href'].startswith('http://') == False and link['href'].startswith('https://') == False :


                        if currentDomain.endswith('/') :
                            currentDomain = currentDomain[:-1]

                        if link['href'].startswith('/') :
                            link['href'] = link['href'][1:]

                        link['href'] = self.urlparse.scheme + '://' + currentDomain + '/' + link['href']

                        link = link['href']

                        internalLinks.append(link)


                    # external link
                    elif currentDomain not in link['href'] and ( link['href'].startswith('http://') or link['href'].startswith('https://') ) :

                        link = link['href']

                        externalLinks.append(link)


                    # internal link non rel
                    elif currentDomain in link['href'] :

                        if link['href'].startswith('http://') == False and link['href'].startswith('https://') == False:
                        
                            if currentDomain.endswith('/') :
                                currentDomain = currentDomain[:-1]

                            if link['href'].startswith('/') :
                                link['href'] = link['href'][1:]

                            link['href'] = self.urlparse.scheme + '://' + currentDomain + '/' + link['href']

                        link = link['href']

                        internalLinks.append(link)

        return internalLinks, externalLinks


    def inQueue(self, url):
        '''
        test to see if the url is part of the pending list
        '''
        #not in pending list
        result = False   
        connection = self.engine.connect()
        result = connection.execute( "select count(*) as total from pending where `url`=%s", (url) )
        for row in result:
            if( row['total']) > 0:
                result = True
        connection.close()
        return result


    def writeLinksToPending(self, url):
        '''
        add the link to the pending table
        '''
        connection = self.engine.connect()
        connection.execute( "insert into pending (url, dateCreated, dateUpdated) values ( %s, NOW(), NOW() )" , (url) )
        connection.close()


    def run(self):
        '''
        thread run, check url
        '''
        #get the item from the queue
        item = self.queue.get()
        
        #while there is an item in the current queue
        while self.running == True:                 

            try:

                #the current url
                url = item['url']
                currentDomain = self.getCurrentDomain(url)

                #fetch the html
                data = self.fetch(url)

                #only if we have a page
                if data != None:

                    #log the current url we are scraping
                    #logging.info('[+] Thread: ' + self.threadNum + ' - Success fetched: %s', url)
                    print('[+] Thread: ' + self.threadNum + ' - Success fetched: %s', url);

                    #create the beautifulSoup object
                    bsObj = BeautifulSoup(data, 'lxml')

                    #get the internal and external links
                    internalLinks, externalLinks = self.getLinks(bsObj, url)
                    
                    # only scrape pages that are relative to the start page
                    for i in internalLinks:
                        #if self.inQueue(i) == False:
                        #self.writeLinksToPending(i)
                        self.queue.put({ 'url' : i })

                    #add to the queue of external links
                    for i in externalLinks:
                        #if self.inQueue(i) == False:
                        #self.writeLinksToPending(i)
                        self.queue.put({ 'url' : i })
                    
                    time.sleep(1)

                #set the next item for the while loop
                item = self.queue.get()                

            except Empty as e:
                print(' ')
                print(' -------- Thread empty ' + self.threadNum + ' url: ' + url + ' -------- ');
                print(e)
                print(' ')
                
                print("-"*60)
                traceback.print_exc(file=sys.stdout)
                print("-"*60)

                self.running = False

            except Exception as e:
                print(' ')
                print(' -------- Thread Running exception ' + self.threadNum + ' url: ' + url + ' -------- ');
                print(e)
                print(' ')

                print("-"*60)
                traceback.print_exc(file=sys.stdout)
                print("-"*60)

                continue

        self.queue.task_done()  
