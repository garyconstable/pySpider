# pySpider

Python web crawler / spider - the app is given a seed url then scrapes links from all pages it finds.

#### Current features
+ worker threads all creawling at the same time
+ shared list of visited links
+ mysql database access to save
  + urls
  + meta titles
  + meta description
  + occurrence of repeating phrazes

##### Database tables:

```
CREATE TABLE `phrazes` (
  `phrazes_id` int(11) unsigned NOT NULL AUTO_INCREMENT,
  `phraze` varchar(200) COLLATE utf8_unicode_ci DEFAULT NULL,
  PRIMARY KEY (`phrazes_id`)
) ENGINE=MyISAM AUTO_INCREMENT=12559 DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;
```

```
CREATE TABLE `url_phraze_pivot` (
  `id` int(11) unsigned NOT NULL AUTO_INCREMENT,
  `urls_id` int(11) DEFAULT NULL,
  `phrazes_id` int(11) DEFAULT NULL,
  `occurrences` int(11) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=MyISAM AUTO_INCREMENT=16045 DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;
```

```
CREATE TABLE `urls` (
  `urls_id` int(11) unsigned NOT NULL AUTO_INCREMENT,
  `url` text COLLATE utf8_unicode_ci,
  PRIMARY KEY (`urls_id`)
) ENGINE=MyISAM AUTO_INCREMENT=7811 DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;
```

##### To run the app you will need to include the missing database connection file:

```
import pymysql.cursors
conn = pymysql.connect(host='', unix_socket='/tmp/mysql.sock', user='', passwd='', db='')
```
  
##### With all dependencies installed run:

```
python3 spider.py
```
