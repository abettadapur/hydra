import multiprocessing
import gevent
import gevent.monkey
from gevent.pool import Pool
from copy_state_db import CopyStateDB
from faster_ordered_dict import FasterOrderedDict
from pymongo import ReadPreference
import utils
import logging
import datetime
import time
import os
import sys

log = utils.get_logger(__name__)


class Stats(object):
    def __init__(self):
        self.start_time = self.adj_start_time = time.time()
        self.inserted = 0
        self.batch = 0
        self.total_docs = None
        self.duplicates = 0 # not a true count of duplicates; just an exception count
        self.exceptions = 0
        self.retries = 0

    def log(self, adjusted=False):
        start_time = self.adj_start_time if adjusted else self.start_time
        total = self.total_docs
        if total == 0:
            total = 1
        qps = int(float(self.inserted) / (time.time() - start_time))
        pct = int(float(self.inserted)/total*100.0)
        text = ("%d%% | %d / %d copied | %d/sec | %d dupes | %d exceptions | %d retries | %d batch" %
                 (pct, self.inserted, self.total_docs, qps, self.duplicates,
                  self.exceptions, self.retries, self.batch))

        log.debug(text)
        sys.stdout.write("\r"+text)
        sys.stdout.flush()


def _ttl_stats_worker(stats):
    while True:
        stats.log()
        gevent.sleep(0.1)


def adjust_ttl_batch_worker(source_collection, seconds, ids, stats):


    stats.batch += len(ids)
    cursor = source_collection.find({'_id': {'$in': ids}})
    cursor.batch_size(len(ids))
    for doc in cursor:
        time = doc['e']
        id = doc['_id']
        newtime = time+datetime.timedelta(seconds=seconds)
        doc['e'] = newtime
        source_collection.update({'_id': id}, {"$set": {'e': newtime}}, upsert=False)
        stats.inserted += 1


def update_ttls(source, state_path, seconds):

    gevent.monkey.patch_socket()

    source_client = utils.mongo_connect(source['host'], source['port'],
                                        ensure_direct=True,
                                        max_pool_size=30,
                                        read_preference=ReadPreference.SECONDARY,
                                        document_class=FasterOrderedDict)

    source_collection = source_client[source['db']][source['collection']]
    if source_client.is_mongos:
        raise Exception("for performance reasons, sources must be mongod instances; %s:%d is not",
                        source['host'], source['port'])

    if seconds < 0:
        log.info("Skipping update, TTL less than 0")
        return

    stats = Stats()
    stats.total_docs = int(source_collection.count())

    ids = []
    cursor = source_collection.find(fields=["_id"], snapshot=True, timeout=False)
    cursor.batch_size(5000)
    insert_pool = Pool(40)
    stats_greenlet = gevent.spawn(_ttl_stats_worker, stats)

    for doc in cursor:
        _id = doc["_id"]

        ids.append(_id)
        if len(ids) % 250 == 0:
            outgoing_ids = ids
            ids = []
            insert_pool.spawn(adjust_ttl_batch_worker,
                              source_collection=source_collection,
                              seconds=seconds,
                              ids=outgoing_ids,
                              stats=stats)

        gevent.sleep()

    if len(ids) > 0:
        adjust_ttl_batch_worker(source_collection=source_collection,
                                seconds=seconds,
                                ids=ids,
                                stats=stats)


    insert_pool.join()
    stats.log()
    stats_greenlet.kill()
    log.info("Finished TTL adjust")






def update_ttls_parent(sources, state_db, args):

    process_names = {repr(source): "%s:%d" % (source['host'], source['port'])
                     for source in sources}

    processes = []
    for source in sources:
        name = process_names[repr(source)]
        process = multiprocessing.Process(target=update_ttls,
                                          name=name,
                                          kwargs=dict(source=source,
                                                      state_path=state_db._path,
                                                      seconds=args.seconds))

        process.start()
        processes.append(process)

        utils.wait_for_processes(processes)


if __name__ == '__main__':
    # NOTE: we are not gevent monkey-patched here; only child processes are monkey-patched,
    #       so all ops below are synchronous

    # parse command-line options
    import argparse

    parser = argparse.ArgumentParser(description='Adjusts TTL values for messages in the database.')
    parser.add_argument(
        '--source', type=str, required=True, metavar='URL',
        help='source to read from; can be a file containing sources or a url like: host[:port]/db/collection; '
             'e.g. localhost:27017/prod_maestro.emails')
    parser.add_argument(
        '--seconds', type=int, required=True, metavar="Minutes",
        help='Adjust the TTL of the messages database by N minutes'
    )
    args = parser.parse_args()

    # parse source
    if os.path.exists(args.source):
        sources = utils.parse_source_file(args.source)
    else:
        sources = [utils.parse_mongo_url(args.source)]

    # initialize sqlite database that holds our state (this may seem like overkill,
    # but it's actually needed to ensure proper synchronization of subprocesses)
    args.state_db = '%s.%s.db' % (sources[0]['db'], sources[0]['collection'])



    state_db_path = os.path.join(os.path.dirname(os.path.realpath(__file__)),
                                     args.state_db)

    log.info('using state db %s' % state_db_path)
    state_db_exists = os.path.exists(state_db_path)
    state_db = CopyStateDB(state_db_path)
    if not state_db_exists:
        state_db.drop_and_create()

    # do the real work
    update_ttls_parent(sources, state_db, args)
    log.info("SUCCESS!!")






