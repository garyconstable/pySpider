

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




class Worker(Thread):


    def __init__( self, queue, threadNum, cur ):
        '''
        init with the queue and set the logger - this worker is a thread extended class
        '''
        Thread.__init__(self)
        self.threadNum = str(threadNum)
        self.cursor = cur      
        self.visitedLinks = []
        self.queue = queue;
        self.running = True
        self.maxException = 9;
        self.exceptionCount = 0;


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
        #domain = '{uri.scheme}://{uri.netloc}/'.format(uri=parsed_uri)
        return parsed_uri.netloc


    def saveLink(self, url, title, description):
        '''
        see if the url exists
        '''
        sql = 'select * from urls where url = %s'
        self.cursor.execute(sql, (url))
        r = self.cursor.fetchone()

        url_id = 0

        if r == None:
            self.cursor.execute('insert into urls (url) values (%s)', url)
            self.cursor.execute('select LAST_INSERT_ID()')
            url_id = self.cursor.fetchone()[0]
        else:
            url_id = r[0]


        '''
        see if the title exists
        '''
        sql = 'select * from phrazes where phraze = %s'
        self.cursor.execute(sql, (title))
        r = self.cursor.fetchone()

        title_id = 0

        if r == None:
            self.cursor.execute('insert into phrazes (phraze) values (%s)', title)
            self.cursor.execute('select LAST_INSERT_ID()')
            title_id = self.cursor.fetchone()[0]
        else:
            title_id = r[0]


        '''
        add title to piviot
        '''
        sql = 'select * from url_phraze_pivot where urls_id = %s and phrazes_id = %s'
        self.cursor.execute(sql, (url_id, title_id))
        r = self.cursor.fetchone()
        if r == None:
            self.cursor.execute('insert into url_phraze_pivot (urls_id, phrazes_id, occurrences) values (%s, %s, 0)', (url_id, title_id))
        else:
            self.cursor.execute('update url_phraze_pivot set occurrences = occurrences +1 where urls_id = %s and phrazes_id = %s ', (url_id, title_id))


        '''
        see if the description exists
        '''
        sql = 'select * from phrazes where phraze = %s'
        self.cursor.execute(sql, (description))
        r = self.cursor.fetchone()

        description_id = 0

        if r == None:
            self.cursor.execute('insert into phrazes (phraze) values (%s)', description)
            self.cursor.execute('select LAST_INSERT_ID()')
            description_id = self.cursor.fetchone()[0]
        else:
            description_id = r[0]


        '''
        add title to piviot
        '''
        sql = 'select * from url_phraze_pivot where urls_id = %s and phrazes_id = %s'
        self.cursor.execute(sql, (url_id, description_id))
        r = self.cursor.fetchone()
        if r == None:
            self.cursor.execute('insert into url_phraze_pivot (urls_id, phrazes_id, occurrences) values (%s, %s, 0)', (url_id, description_id))
        else:
            self.cursor.execute('update url_phraze_pivot set occurrences = occurrences +1 where urls_id = %s and phrazes_id = %s ', (url_id, description_id))


    def getMetaTitle(self, html):
        '''
        extract the page meta title
        '''
        title = html.title.string
        return title


    def getMetaDescription(self, html):
        '''
        extract the page meta description
        '''
        description = html.findAll(attrs={"name":"description"})
        return description[0]['content']


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
        self.visitedLinks.append(uri)

        try:
            h = urlopen(uri)
            x =  h.info()
            if 'text/html' in x['Content-Type'].lower():
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

                        link['href'] = self.urlparse.scheme + '//' + currentDomain + '/' + link['href']

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

                            link['href'] = self.urlparse.scheme + '//' + currentDomain + '/' + link['href']

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

                #make sure we have not yet visited
                if( url not in self.visitedLinks ):

                    #fetch the html
                    data = self.fetch(url)

                    if data == None:
                        
                        #log that we could not get data from the url
                        #logging.info('[-] Thread: ' + self.threadNum + ' - Could not fetch: %s because type != text/html', url)
                        a = 2

                    else:

                        #log the current url we are scraping
                        logging.info('[+] Thread: ' + self.threadNum + ' - Success fetched: %s', url)

                        #create the beautifulSoup object
                        bsObj = BeautifulSoup(data, 'lxml')

                        #get the internal and external links
                        internalLinks, externalLinks = self.getLinks(bsObj, url)
             
                        #get the meta title
                        metaTitle = self.getMetaTitle(bsObj)

                        #get the meta desciption
                        metaDescription = self.getMetaDescription(bsObj)
                        
                        #savet the link in the database
                        self.saveLink(url, metaTitle, metaDescription)

                        # only scrape pages that are relative to the start page
                        for i in internalLinks:
                            self.queue.put({ 'url' : i })

                        #add to the queue of external links
                        for i in externalLinks:
                            self.queue.put({ 'url' : i })

                        #have a quick nap
                        time.sleep(2)

                #set the next item for the while loop
                item = self.queue.get()                

            except Empty:
                self.running = False

            except:
                self.exceptionCount = self.exceptionCount + 1
                if self.exceptionCount == self.maxException:
                    self.running = False
                else:
                    continue

        self.queue.task_done() 

                

        
