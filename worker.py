from bs4 import BeautifulSoup
import logging
import MySQLdb
import os

#logging.basicConfig(level=logging.DEBUG)

queue_limit = 10


class Worker:
    def __init__(self):
        self.db_engine = MySQLdb.connect(
            host='127.0.0.1',
            user='root',
            passwd='root',
            database='pySpider'
        )

    @staticmethod
    def get_queue_length():
        return len([1 for x in list(os.scandir("pending")) if x.is_file()])

    def queue_runner(self, attempt=0):
        print("[+] Run Batch: " + str(attempt) )
        queue_size = self.get_queue_length()
        if queue_size > 0:
            self.process_queue()
            attempt = attempt + 1
            queue_size = self.get_queue_length()
            if queue_size > 0:
                self.queue_runner(attempt)

    def process_queue(self):
        pages = []
        limit = 0
        for x in list(os.scandir("pending")):
            if x.is_file():
                error = False
                file, ext = os.path.splitext(os.path.basename(x))
                try:
                    with open(x, 'rb') as f:
                        contents = f.read()

                    pages.append(contents)
                    filename = "processed/" + file + ext
                    file = open(filename, 'wb')
                    file.write(contents)
                    file.close()
                    os.remove(x)
                except:
                    error = True
                    # print("==> could not open and write file")

                limit = limit + 1

                if limit == queue_limit:
                    break

        for page in pages:
            bs_obj = BeautifulSoup(page, 'lxml')
            external_links = self.get_external_links(bs_obj)
            for link in external_links:
                self.save_link(link)

    @staticmethod
    def get_external_links(page):
        external_links = []
        for link in page.findAll('a', href=True):
            if link.has_attr('href'):
                if link['href'].startswith('#') is False and link['href'] != '/':
                    if link['href'].startswith('http://') or link['href'].startswith('https://'):
                        link = link['href']
                        external_links.append(link)
        return external_links

    def save_link(self, url):
        error = False
        try:
            sql = "insert into queue (url) values ('" + url + "')"
            cursor = self.db_engine.cursor(MySQLdb.cursors.DictCursor)
            cursor.execute(sql)
            self.db_engine.commit()
            cursor.close()
        except:
            error = True
            # print('===> error(sql): ' + sql)


if __name__ == '__main__':
    w = Worker()
    w.queue_runner(1)
