from __future__ import print_function

import pymongo
import sys

def get_client(ip_address, connection_string):
    try:
        connection = pymongo.MongoClient(host=ip_address, port=27017)
    except pymongo.errors.ConnectionFailure:
        print("Could not connect to Catalog Database!")
        sys.exit(1)
    return connection

def main():
    if len(sys.arv) <=1:
        usg = "usage: python {0} {1} {2}"
        print(usg.format(sys.argv[0], "catalog db ip address", "connection string"))
        sys.exit(1)
    client = get_client(sys.argv[1], sys.arv[2])
    db = client['marconi_shards']
    db.shards.update({}, {'$set': {'u': sys.argv[2]}})

if __name__=="__main__":
    main()

