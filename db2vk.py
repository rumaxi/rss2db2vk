import pymongo as pm
import schedule
import logging as log
import feedparser
import hashlib
import time
import requests
import json
import random

DBSTRING    = 'mongodb://localhost:27017/'
DBNAME      = 'news'
PUBLICS_T   = 'publics'

FEED_T_SUFF = '_feed'
SEEN_T_SUFF = '_seen'

url_api_getserver       = 'https://api.vk.com/method/photos.getWallUploadServer'
url_api_save            = 'https://api.vk.com/method/photos.saveWallPhoto'
url_api_post            = 'https://api.vk.com/method/wall.post'


def get_image(post):
    img = False
    try:
        log.debug('href picture ?')
        img = post['href']
    except:
        pass
    if not img:
        try:
            log.debug('links picture ?')
            img = [ x['href'] for x in post['links'] if 'image' in x['type'] ][0]
        except:
            pass
    if not img:
        log.debug('no picture')
        log.debug(post)
    return img

def upload_image(link, gid, token):
    fn = link.split('/')[-1]
    try:
        r = requests.get(link, stream = True)
    except:
        log.error('error getting image from {link}'.format(link = link))
        return False
    files = {'photo': (fn, r.raw, 'multipart/form-data')}
    pic_api_data = {
         'group_id'             : gid,
         'access_token'         : token,
    }
    url = ''
    try:
        r = requests.post(url_api_getserver, pic_api_data)
        url = json.loads(r.text)['response']['upload_url']
    except:
        log.error('error getting image server for {link}'.format(link = link))
    if url:
        try:
            r = requests.post(url, files=files)
            pic_save_api_data = {
                'hash'          :       json.loads(r.text)['hash'],
                'photo'         :       json.loads(r.text)['photo'],
                'server'        :       json.loads(r.text)['server'],
                'group_id'      :       gid,
                'access_token'  :       token,
            }
            r = requests.post(url_api_save, pic_save_api_data)
            pic_id = json.loads(r.text)['response'][0]['id']
            return pic_id
        except:
            log.error('error uploading image to vk {link}'.format(link = link))
            return False

def post_vk(post, pic_id, gid, token):
     post_api_data = {
         'from_group'   :       '1',
         'owner_id'     :       '-'+gid,
         'message'      :       post['title_detail']['value']+' '+post['link'],
         'attachments'  :       pic_id,
         'access_token' :       token,
         'group_id'     :       gid,
     }
     r = requests.post(url_api_post, post_api_data)
     return r.text

def update():
    try:
        client = pm.MongoClient(DBSTRING)
        db = client[DBNAME]
    except:
        log.error('error connecting to db')
        return 0
    publics = db[PUBLICS_T].find()

    for p in publics:
        prob = p['period']
        if random.randint(1,prob) == random.randint(1,prob):
            log.debug('update() -> True')
            pub_name = p['name']
            token    = p['token']
            gid      = p['gid']
            feed_t = db[pub_name+FEED_T_SUFF]
            post = feed_t.find_one({'status':'new'})
            left = feed_t.find({'status':'new'}).count()
            if post:
                log.debug('got post, getting image')
                img = get_image(post)
                res = []
                if img:
                    log.debug('got image, uploading to vk')
                    pic_id = upload_image(img, gid, token)
                    if pic_id:
                        log.debug('uploaded picture, posting')
                        res = json.loads (post_vk(post, pic_id, gid, token))
                        if 'error' in res:
                            feed_t.update({ '_id': post.get('_id') }, { "$set": { 'status': 'error' } })
                            log.error('posting error. {err}'.format(err = res['error']['error_msg']))
                        if 'response' in res:
                            feed_t.update({ '_id': post.get('_id') }, { "$set": { 'status': 'posted' } })
                            log.log(31,'[ {pub_name} ] successfully posted, {left} queued'.format(pub_name = pub_name, left = left))
                if not res:
                            feed_t.update({ '_id': post.get('_id') }, { "$set": { 'status': 'error' } })
                            log.error('[ {pub_name} ] unknown error, {left} queued'.format(pub_name = pub_name, left = left))
        else:
            log.debug('update() -> False')
        client.close()


log.basicConfig(level=log.WARNING, format='%(asctime)s - %(message)s')
#log.basicConfig(level=log.DEBUG, format='%(asctime)s - %(message)s')
log.log (31, 'starting')
schedule.every(1).minutes.do(update)
while 1:
    schedule.run_pending()
    time.sleep(1)
