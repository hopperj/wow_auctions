# @Author: Jason Hopper <hopperj>
# @Date:   2016-11-11
# @Email:  hopperj@ampereinnotech.com
# @Last modified by:   hopperj
# @Last modified time: 2016-11-29
# @License: GPL3



import json
import requests
import pymongo
import click
import logging
from datetime import datetime

class Config:
    def __init__(self):
        pass


log_levels = {
    'debug': logging.DEBUG,
    'info': logging.INFO,
    'warning': logging.WARNING,
    'error': logging.ERROR,
    'critical': logging.CRITICAL
}


@click.group(invoke_without_command=True)
@click.option('--log-level', default='info')
@click.option('--log', default=False)
@click.option('--db-addr', default='localhost', required=False)
@click.option('--db-port', default=27017, required=False)
@click.option('--db-name', default='wow_auctions', required=False)
@click.option('--db-url-name', default='urls', required=False)
@click.option('--db-data-name', default='auctions', required=False)
@click.option('--db-item-name', default='items', required=False)
@click.option('--api-key', required=True)
@click.pass_context
def run(ctx, log_level, log, db_addr, db_port, db_name, db_url_name, db_data_name, db_item_name, api_key):


    log_level = log_levels[log_level]

    logging.getLogger(__name__)
    if log:
        logging.basicConfig(filename=log, filemode='a', format='%(asctime)s %(levelname)s: %(message)s', datefmt='%Y-%m-%dT%H:%M:%S', level=log_level)
    else:
        logging.basicConfig(format='%(asctime)s %(levelname)s: %(message)s', datefmt='%Y-%m-%dT%H:%M:%S', level=log_level)


    ctx.obj = Config()

    connection = pymongo.MongoClient(
    db_addr,
    db_port,
    )

    ctx.obj.db = connection[db_name]
    ctx.obj.urls_collection = ctx.obj.db[db_url_name]
    ctx.obj.data_collection = ctx.obj.db[db_data_name]
    ctx.obj.item_collection = ctx.obj.db[db_item_name]

    ctx.obj.api_key = api_key


# @run.command()
# @click.argument('item_id', type=int, required=True)
# @click.pass_context
def get_item(item_collection, item_id, api_key):

    if item_collection.find({'id':item_id}).count():
        return

    logging.info('getting item: %d'%item_id)
    item_data = json.loads(
        requests.get(
            'https://us.api.battle.net/wow/item/%d?locale=en_US&apikey=%s'%(item_id, api_key)
        ).text
    )


    item_collection.insert(item_data)



@run.command()
@click.pass_context
def pull(ctx):

    logging.debug('Getting new AH data')
    url_data = json.loads(
        requests.get(
            'https://us.api.battle.net/wow/auction/data/wildhammer?locale=en_US&apikey=%s'%ctx.obj.api_key
        ).text
    )['files'][0]


    if ctx.obj.urls_collection.find({'lastModified':url_data['lastModified']}).count():
        logging.info('No update found')
        return


    auction_data = json.loads(
        requests.get(
            url_data['url']
        ).text
    )

    ctx.obj.data_collection.insert(
        auction_data,
    )
    ctx.obj.urls_collection.insert(url_data)

    logging.info('Data pulled for timestamp: %d'%url_data['lastModified'])


    for auction in auction_data['auctions']:
        # print('doing item: ',auction['item'])
        # print(auction['item'])
        get_item(ctx.obj.data_collection, auction['item'], ctx.obj.api_key)
