import pymongo as pm
import schedule
import logging as log
import feedparser 
import hashlib
import time

DBNAME      = 'news'
PUBLICS_T   = 'publics'
FEED_T_SUFF = '_feed'
SEEN_T_SUFF = '_seen'

def update():
    try:
        client = pm.MongoClient('mongodb://localhost:27017/')
        db = client[DBNAME]
    except:
        log.error('error connecting to db') 
        return 0
    publics = db[PUBLICS_T].find()
    new = 0
    for p in publics:
        pub_name = p['name']
        feed_t = db[pub_name+FEED_T_SUFF]
        seen_t = db[pub_name+SEEN_T_SUFF]
        for source in p['sources']: 
            if source['type'] == 'rss':
                feed = feedparser.parse(source['link'])
                for item in feed['entries']:
                    title   =   item.title
                    link    =   item.link
                    id_     =   str.encode(title+link)
                    md      =   hashlib.md5(id_).hexdigest()
                    if not seen_t.find_one({'hash':md}):
                        item['status'] = "new"
                        feed_t.insert_one(item)
                        seen_t.insert_one({'hash':md})
                        new += 1 
            else:
                log.error('wrong source type')
    if new:
        log.log(31, '[ {pub_name} ] {new} new posts'.format(new = new, pub_name = pub_name))
    client.close()


log.basicConfig(level=log.WARNING, format='%(asctime)s - %(message)s')
log.log (31, 'starting')
schedule.every(1).minutes.do(update)
while 1:
    schedule.run_pending()
    time.sleep(1)

