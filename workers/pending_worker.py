import time as time_
import MySQLdb
import requests

per_batch = 60


def current_milli_time():
    return str(round(time_.time() * 1000))


class SavePage:
    def __init__(self):
        self.db_engine = MySQLdb.connect(
            host='127.0.0.1',
            user='root',
            passwd='root',
            database='pySpider'
        )

    def build_queue(self):
        """
        Get the next set of items from the queue
        """
        cursor = self.db_engine.cursor(MySQLdb.cursors.DictCursor)
        cursor2 = self.db_engine.cursor(MySQLdb.cursors.DictCursor)
        cursor.execute('select url, id from pending order by id asc limit ' + str(per_batch))
        rows = cursor.fetchall()
        pending = []
        for row in rows:
            pending.append({
                'url': row['url']
            })
            cursor2.execute('DELETE FROM pending where id = "'+str(row['id'])+'" ')
            self.db_engine.commit()
        return pending

    @staticmethod
    def encode_link(link):
        """
        Encode the links
        """
        ip = bytes(link, "UTF-8")
        link = ip.decode("ascii", "ignore")
        return link

    def fetch(self, uri):
        """
        Fetch the page
        """
        url = self.encode_link(uri)
        try:
            headers = {
                'User-Agent': "Mozilla/5.0 (X11; Linux i686) AppleWebKit/537.17 (KHTML, like Gecko) "
                              "Chrome/24.0.1312.27 Safari/537.17 "
            }
            r = requests.get(url, headers=headers, timeout=1)
            return r.content
        except KeyboardInterrupt:
            exit()
        except:
            return None

    def process_queue(self, attempt=0):
        """
        Work through the queue and save the html pages
        """
        pending = s.build_queue()
        index = 1
        for item in pending:
            page = self.fetch(item['url'])
            print("[+] Batch: " + str(attempt) + ", Page " + str(index) + ", Url: " + str(item['url']))
            index = index + 1
            if page is not None:
                filename = "pending/" + current_milli_time() + ".txt"
                file = open(filename, 'wb')
                file.write(page)
                file.close()

    def get_queue_length(self):
        """
        Get the Queue Length
        """
        cursor = self.db_engine.cursor(MySQLdb.cursors.DictCursor)
        cursor.execute('select count(*) as count from pending')
        rows = cursor.fetchall()
        for row in rows:
            return row['count']

    def queue_runner(self, attempt=0):
        """
        Run the queue
        """
        queue_size = self.get_queue_length()
        if queue_size > 0:
            s.process_queue(attempt)
            attempt = attempt + 1
            queue_size = self.get_queue_length()
            if queue_size > 0:
                self.queue_runner(attempt)


if __name__ == '__main__':
    s = SavePage()
    s.queue_runner(1)
