
import MySQLdb
from urllib.parse import urlsplit


def save_domain(db, url):
    sql = "insert IGNORE into domains (domain) values ('" + url + "')"
    cursor = db.cursor(MySQLdb.cursors.DictCursor)
    cursor.execute(sql)
    db.commit()
    cursor.close()
    print(url)


def extract_from_string(url):
    base_url = "{0.scheme}://{0.netloc}/".format(urlsplit(url))
    return base_url


def get_domains():
    db = MySQLdb.connect(
        host='127.0.0.1',
        user='root',
        passwd='root',
        database='pySpider'
    )

    sql = " select * from queue order by id asc; "
    cursor = db.cursor(MySQLdb.cursors.DictCursor)
    cursor.execute(sql)
    rows = cursor.fetchall()

    for row in rows:
        sql = "insert into pending (url) values ('" + row['url'] + "')"
        cursor2 = db.cursor(MySQLdb.cursors.DictCursor)
        cursor2.execute(sql)
        db.commit()
        cursor2.close()

        save_domain(db, extract_from_string(row['url']))

    print()
    print("==> complete")
    exit()


if __name__ == '__main__':
    get_domains()
