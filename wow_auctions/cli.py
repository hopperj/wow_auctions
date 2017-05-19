# @Author: Jason Hopper <hopperj>
# @Date:   2016-11-11
# @Email:  hopperj@ampereinnotech.com
# @Last modified by:   hopperj
# @Last modified time: 2016-12-10
# @License: GPL3



import json
import requests
import pymongo
import click
import logging
from datetime import datetime
from multiprocessing.dummy import Pool as ThreadPool
import numpy as np
from numpy import average

class Config:
    def __init__(self):
        pass

gold_conv = 1e4

log_levels = {
    'debug': logging.DEBUG,
    'info': logging.INFO,
    'warning': logging.WARNING,
    'error': logging.ERROR,
    'critical': logging.CRITICAL
}


@click.group(invoke_without_command=True)
@click.option('--log-level', default='debug')
@click.option('--log', default=False)
@click.option('--db-uri', required=True)
@click.option('--db-url-name', default='urls', required=False)
@click.option('--db-data-name', default='auctions', required=False)
@click.option('--db-item-name', default='items', required=False)
@click.option('--db-item-stats-name', default='item_stats', required=False)
@click.option('--api-key', required=True)
@click.pass_context
def run(ctx, log_level, log, db_uri, db_url_name, db_data_name, db_item_name, db_item_stats_name, api_key):


    log_level = log_levels[log_level]

    logging.getLogger(__name__)
    if log:
        logging.basicConfig(filename=log, filemode='a', format='%(asctime)s %(levelname)s: %(message)s', datefmt='%Y-%m-%dT%H:%M:%S', level=log_level)
    else:
        logging.basicConfig(format='%(asctime)s %(levelname)s: %(message)s', datefmt='%Y-%m-%dT%H:%M:%S', level=log_level)


    ctx.obj = Config()

    connection = pymongo.MongoClient(
        db_uri,
    )

    ctx.obj.db = connection[db_uri.split('/')[-1]]
    ctx.obj.urls_collection = ctx.obj.db[db_url_name]
    ctx.obj.data_collection = ctx.obj.db[db_data_name]
    ctx.obj.item_collection = ctx.obj.db[db_item_name]
    ctx.obj.item_stats_collection = ctx.obj.db[db_item_stats_name]

    ctx.obj.api_key = api_key


def get_item(item_url):
    item_data = json.loads(
        requests.get(
            item_url
        ).text
    )
    return item_data


def get_all_items(all_items, item_collection, api_key):


    items_to_get = []

    for item_id in all_items:
        if not item_collection.find({'id':item_id}).count():
            items_to_get.append('https://us.api.battle.net/wow/item/%s?locale=en_US&apikey=%s'%(item_id, api_key))

    pool = ThreadPool(5)
    results = pool.map(get_item, items_to_get)
    pool.close()
    pool.join()
    for result in results:
        if 'reason' in result:
            continue
        logging.info('Inseting data for item: %s'%result['name'])
        item_collection.update(result, result, upsert=True)



def get_data_url(url):
    logging.debug('Getting new AH data')
    url_data = json.loads(
        requests.get(
            url
        ).text
    )

    url_data = url_data['files'][0]

    timestamp = datetime.fromtimestamp(url_data['lastModified']/1e3)
    del url_data['lastModified']
    url_data['timestamp'] = timestamp

    return url_data
        

def pull_auction_data(url):
    logging.debug('Pulling new data...')
    auction_data = json.loads(
        requests.get(
            url
        ).text
    )
    with open('/data/wow_auction_archive/data_{}.json'.format(datetime.now().strftime('%Y-%m-%d_%H:%M')), 'w') as f:
        f.write(json.dumps(auction_data))
    return auction_data

    
def process_data(auction_data, data_url, ctx):
    parsed_auctions = []
    logging.info('Processing auction data')

    for auction in auction_data['auctions']:
        auction.update(
            {
                'timestamp':data_url['timestamp'],
            }
        )
        
        parsed_auctions.append(
            auction
        )
    

    tmp = ctx.obj.data_collection.find( auction_data['auctions'][0] )

    if tmp.count():
        logging.info('Auction data already stored')
        logging.info(tmp.count())
        logging.info(tmp)
        logging.info(auction_data['auctions'][0])
        return
    
    logging.info('Processed %d auctions for timestamp %s'%(len(parsed_auctions), data_url['timestamp']))


    all_items = [ auction['item'] for auction in auction_data['auctions'] ]
    get_all_items(all_items, ctx.obj.item_collection, ctx.obj.api_key)
    logging.info('Updated items')

    bulk = ctx.obj.data_collection.initialize_unordered_bulk_op()

    logging.debug("inserting %d auctions"%len(parsed_auctions))
    [bulk.insert(e) for e in parsed_auctions ]
    bulk.execute()

    logging.info('Auctions inserted')

    logging.debug('grouping items')

    item_stats = [ calc_stats(i) for i in group_items(parsed_auctions).values() ]
    if ctx.obj.item_stats_collection.find({'timestamp':item_stats[0]['timestamp']}).count():
        logging.info('Skipping stats insert. Already done')
        return
    logging.debug('Inserting stats objects')
    bulk = ctx.obj.item_stats_collection.initialize_unordered_bulk_op()
    [bulk.insert(e) for e in item_stats ]
    bulk.execute()
    #ctx.obj.item_stats.insert_many(item_stats)

def group_items(auctions):
    item_auctions = {}
    for i,auction in enumerate(auctions):
        if str(auction['item']) not in item_auctions:
            item_auctions[ str(auction['item']) ] = []
        item_auctions[ str(auction['item']) ].append( auction )

    return item_auctions

def calc_stats(item_auctions):
    buyouts = [ max(a['buyout'], a['bid'])/float(a['quantity']) for a in item_auctions]
    stats = {
        'item':item_auctions[0]['item'],
        'timestamp':item_auctions[0]['timestamp'],
        'min':min(buyouts)/gold_conv,
        'average':average(buyouts)/gold_conv,
        'max':max(buyouts)/gold_conv,
        'count':sum([ a['quantity'] for a in item_auctions ]),
        'std':np.std(buyouts)/gold_conv
    }
    
    return stats

@run.command()
@click.pass_context
def pull(ctx):
    url = 'https://us.api.battle.net/wow/auction/data/wildhammer?locale=en_US&apikey=%s'%ctx.obj.api_key
    logging.info('Pulling AH data for url: %s'%url)
    data_url = get_data_url(url)
    auction_data = pull_auction_data(data_url['url'])
    process_data(auction_data, data_url, ctx)
    logging.info('All done!')

@run.command()
@click.pass_context
def pull_new(ctx):
    url = 'https://us.api.battle.net/wow/auction/data/wildhammer?locale=en_US&apikey=%s'%ctx.obj.api_key
    logging.info('Pulling new AH data')
    data_url = get_data_url(url)

    if ctx.obj.data_collection.find( {'timestamp':data_url['timestamp']}).count():
        logging.info('No update found for timestamp: %s'%data_url['timestamp'])
        return

    auction_data = pull_auction_data(data_url['url'])

    process_data(auction_data, data_url, ctx)
    logging.info('All done!')
