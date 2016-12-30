import pymongo as pm
from rss_sources_it import rsssources

skel = {
    'token'     :   '',
    'gid'       :   '',
    'sources'   :   {},
}

def create(name, dbname, gid, token, sources):
    client = pm.MongoClient('mongodb://localhost:27017/')
    db = client[dbname]
    res = { 'name'  : name,
            'token' : token,
            'gid'   : gid,
            'sources' : sources,
    }
    db.publics.insert_one(res)

sources = []
for s in rsssources:
    res = {}
    res['link'] = s.link
    res['type'] = 'rss'
    res['name'] = s.name
    res['active'] = True
    sources.append(res)
print (sources)


create (
    name    = 'my_it_news',
    token   = '',
    gid     = '135910433',
    sources = sources,
    dbname  = 'news',
)

