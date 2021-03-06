

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


logging.basicConfig(level=logging.DEBUG)


class Worker(Thread):


    def __init__( self, queue, threadNum, visitedLinks, lock ):
        '''
        init with the queue and set the logger - this worker is a thread extended class
        '''
        Thread.__init__(self)
        self.threadNum = str(threadNum)    
        self.visitedLinks = visitedLinks
        self.queue = queue;
        self.urlDetails = []
        self.running = True
        self.lock = lock


        self.excludedDomains = []
        self.excludedDomains.append('www.reddit.com')
        self.excludedDomains.append('www.deviantart.com')
        self.excludedDomains.append('comments.deviantart.com')

        


    def join(self, timeout=None):
        '''
        when the thread joins send the loop end signal
        '''
        self.running = False
        super(Worker, self).join(timeout)


    def getVisitedLinks(self):
        '''
        return the workers current visited links
        '''
        self.lock.acquire()
        tmp = self.visitedLinks
        self.lock.release()
        return tmp


    def setVisitedLinks(self, visitedLinks):
        '''
        add to the workers visited links
        '''
        self.lock.acquire()
        self.visitedLinks.union(visitedLinks)
        self.lock.release()        


    def getCurrentDomain(self, page):
        '''
        get the domain we are crawling
        '''
        parsed_uri = urlparse(page)
        return parsed_uri.netloc


    def getMetaTitle(self, html):
        '''
        extract the page meta title
        '''
        if html.title is not None:
            return html.title.string.encode('utf-8')

        return ""


    def getMetaDescription(self, html):
        '''
        extract the page meta description
        '''
        description = html.findAll(attrs={"name":"description"})
        if len(description) and description[0]['content'] != None:
            return description[0]['content'].encode('utf-8')
        return ""


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
        self.visitedLinks.add(uri)        
        try:
            h = urlopen(uri)
            x =  h.info()
            if x['Content-Type'] != None and 'text/html' in x['Content-Type'].lower():
                return h.read()
            else:
                return None
        except urllib.error.URLError:
            return None

                
    def getUrlDetails(self):
        '''
        get the list of url urlDetails
        '''
        return self.urlDetails


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

                #make sure we have not yet visited
                if( url not in self.visitedLinks and currentDomain not in self.excludedDomains ):

                    #create a lock
                    self.lock.acquire()

                    #fetch the html
                    data = self.fetch(url)

                    #only if we have a page
                    if data != None:

                        #log the current url we are scraping
                        logging.info('[+] Thread: ' + self.threadNum + ' - Success fetched: %s', url)

                        #create the beautifulSoup object
                        bsObj = BeautifulSoup(data, 'lxml')

                        #get the internal and external links
                        internalLinks, externalLinks = self.getLinks(bsObj, url)
                        
                        #get the meta title
                        #metaTitle = self.getMetaTitle(bsObj)

                        #get the meta desciption
                        #metaDescription = self.getMetaDescription(bsObj)
                        
                        #add to the save queue
                        #self.urlDetails.append({
                        #    'url': url,  
                        #    'title': metaTitle,  
                        #    'description': metaDescription  
                        #})
                        
                        # only scrape pages that are relative to the start page
                        for i in internalLinks:
                            if i not in self.visitedLinks: 
                                self.queue.put({ 'url' : i })

                        #add to the queue of external links
                        for i in externalLinks:
                            if i not in self.visitedLinks: 
                                self.queue.put({ 'url' : i })

                    #have a quick nap
                    time.sleep(0.5)

                    #release the lock       
                    self.lock.release()

                #let the other threads join in!
                time.sleep(0.5)
                 
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
