import sys, traceback
from threading import Thread
import urllib
from urllib.request import urlopen
from urllib.parse import urlparse
from bs4 import BeautifulSoup
import logging
from queue import *
import time
import MySQLdb

logging.basicConfig(level=logging.DEBUG)


class Worker(Thread):
    running: bool

    def __init__(self, queue, threadNum, lock, engine):
        """
        init with the queue and set the logger - this worker is a thread extended class
        """
        Thread.__init__(self)
        self.threadNum = str(threadNum)
        self.queue = queue;
        self.running = True
        self.lock = lock
        self.excludedDomains = []
        self.setexcludedeomains()
        self.db_engine = engine

    def setexcludedeomains(self):
        """
        domains that we want to exclude from the search
        """
        self.excludedDomains.append('www.reddit.com')
        self.excludedDomains.append('www.deviantart.com')
        self.excludedDomains.append('comments.deviantart.com')

    def join(self, timeout=None):
        """
        when the thread joins send the loop end signal
        """
        self.running = False
        super(Worker, self).join(timeout)

    @staticmethod
    def getcurrentdomain(page):
        """
        get the domain we are crawling
        """
        parsed_uri = urlparse(page)
        return parsed_uri.netloc

    @staticmethod
    def encodelink(link):
        """
        attempt to fix encoding issues with links
        """
        input = bytes(link, "UTF-8")
        link = input.decode("ascii", "ignore")
        return link;

    def fetch(self, uri):
        """
        url open, check the headers for text/html
        if so return data
        """
        uri = self.encodelink(uri)
        try:
            h = urlopen(uri)
            x = h.info()
            if x['Content-Type'] != None and 'text/html' in x['Content-Type'].lower():
                return h.read()
            else:
                return None
        except urllib.error.URLError:
            return None

    def getlinks(self, page, url):
        """
        find all anchor linsk within the page
        add to either array depending on its http(s) status
        """
        internalLinks = [];
        externalLinks = [];
        currentDomain = self.getcurrentdomain(url)
        self.urlparse = urlparse(url)

        for link in page.findAll('a', href=True):

            if link.has_attr('href'):

                if (link['href'].startswith('#') == False and link['href'] != '/'):

                    # internal link - rel link
                    if currentDomain not in link['href'] and link['href'].startswith('http://') == False and link[
                        'href'].startswith('https://') == False:

                        if currentDomain.endswith('/'):
                            currentDomain = currentDomain[:-1]

                        if link['href'].startswith('/'):
                            link['href'] = link['href'][1:]

                        link['href'] = self.urlparse.scheme + '://' + currentDomain + '/' + link['href']

                        link = link['href']

                        internalLinks.append(link)

                    # external link
                    elif currentDomain not in link['href'] and (
                            link['href'].startswith('http://') or link['href'].startswith('https://')):
                        link = link['href']
                        externalLinks.append(link)

                    # internal link non rel
                    elif currentDomain in link['href']:

                        if link['href'].startswith('http://') == False and link['href'].startswith('https://') == False:
                            if currentDomain.endswith('/'):
                                currentDomain = currentDomain[:-1]

                            if link['href'].startswith('/'):
                                link['href'] = link['href'][1:]

                            link['href'] = self.urlparse.scheme + '://' + currentDomain + '/' + link['href']
                        link = link['href']
                        internalLinks.append(link)
        return internalLinks, externalLinks

    def inqueue(self, url):
        """
        test to see if the url is part of the pending list
        """
        result = False
        cursor = self.db_engine.cursor(MySQLdb.cursors.DictCursor)
        sql = " select count(*) as total from pending where `url`= '" + url + "' "
        cursor.execute(sql)
        rows = cursor.fetchall()
        for row in rows:
            if row['total'] > 0:
                result = True
        cursor.close()
        return result

    def writelinkstopending(self, url):
        """
        add the link to the pending table
        """
        sql = "insert into pending (url, dateCreated, dateUpdated) values ( '" + url + "', NOW(), NOW() )"
        try:
            cursor = self.db_engine.cursor(MySQLdb.cursors.DictCursor)

            cursor.execute(sql)
            self.db_engine.commit()
            cursor.close()
        except:
            print('===> error(sql): ' + sql)

    def run(self):
        """
        thread run, check url
        """
        # get the item from the queue
        item = self.queue.get()

        # while there is an item in the current queue
        while self.running:

            try:

                # the current url
                url = item['url']
                currentDomain = self.getcurrentdomain(url)

                # fetch the html
                data = self.fetch(url)

                # only if we have a page
                if data != None:

                    # log the current url we are scraping
                    # logging.info('[+] Thread: ' + self.threadNum + ' - Success fetched: %s', url)
                    print('[+] Thread: ' + self.threadNum + ' - Success fetched: %s', url);

                    # create the beautifulSoup object
                    bsObj = BeautifulSoup(data, 'lxml')

                    # get the internal and external links
                    internalLinks, externalLinks = self.getlinks(bsObj, url)

                    # only scrape pages that are relative to the start page
                    '''
                    for i in internalLinks:
                        if not self.inqueue(i):
                            self.writelinkstopending(i)
                            # self.queue.put({'url': i})
                    '''

                    # add to the queue of external links
                    for i in externalLinks:
                        if not self.inqueue(i):
                            self.writelinkstopending(i)
                            # self.queue.put({'url': i})

                    time.sleep(1)

                # set the next item for the while loop
                item = self.queue.get()

            except Empty as e:
                print(' ')
                print(' -------- Thread empty ' + self.threadNum + ' url: ' + url + ' -------- ');
                print(e)
                print(' ')

                print("- " * 60)
                traceback.print_exc(file=sys.stdout)
                print("- " * 60)

                self.running = False

            except Exception as e:
                print(' ')
                print(' -------- Thread Running exception ' + self.threadNum + ' url: ' + url + ' -------- ');
                print(e)
                print(' ')

                print("- " * 60)
                traceback.print_exc(file=sys.stdout)
                print("- " * 60)

                continue

        self.queue.task_done()
