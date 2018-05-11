#!/usr/bin/env python

from elasticsearch import Elasticsearch
import pandas as pd
import json
import copy
import time

import concurrent.futures
import asyncio
import functools

import redis

def esRest2Pandas(result, agg1, agg2, aggs):
    names1 = []
    names2 = []
    frames = {}

    keys1 = []
    for idx1, val1 in enumerate(result["aggregations"][agg1]["buckets"]):
        sub = []
        key1 = val1["key"]
        keys1.append(key1)

    for a in aggs: frames[a] = pd.DataFrame(0, index=keys1, columns=keys1)

    for idx1, val1 in enumerate(result["aggregations"][agg1]["buckets"]):
        aggData = val1[agg2]["buckets"]
        key1 = val1["key"]
        for idx2, val2 in enumerate(aggData):
            key2 = val2["key"]
            if val2["doc_count"] > 0:
                for k, v in val2.items():
                    if k in aggs: frames[k].xs(key1)[key2] = v["value"]

    return pd.concat(frames.values(), keys=aggs)

class SmurfParty():
    def __init__(self, **kwargs):
        self.workers        =   22
        self.indexPattern   =   "sessions2-*"
        self.limit          =   5

        self.agg1   =   kwargs.get("agg1", "sourceTeam")
        self.agg2   =   kwargs.get("agg2", "destTeam")
        self.aggs   =   kwargs.get("aggs", [ "dataBytes", "packets", "bytes" ])

        self.qTpl       =   "./query.json"
        self.queries    =   []
        self.smurfs     =   []
        self.createQueries()

        self.es_hosts   =   kwargs.get("es_hosts", ["localhost:9200"])

        self.redis_host =   kwargs.get("redis_host", "localhost")
        self.redis_port =   kwargs.get("redis_port", 6379)
        self.redis_db   =   kwargs.get("redis_db", 0)

        try:
            self.es     =   Elasticsearch(hosts=self.es_hosts)
        except Exception as e:
            raise e

        try:
            self.redis  =   redis.Redis(host=self.redis_host, port=self.redis_port, db=self.redis_db)
        except Exception as e:
            raise e

        self.data       =   []

    def BeerRun(self):
        loop = asyncio.get_event_loop()
        loop.run_until_complete(self.Chug())

    async def Chug(self):
        start   =   int(time.time())
        indices =   self.indexList()

        with concurrent.futures.ThreadPoolExecutor(max_workers=self.workers) as executor:
            loop = asyncio.get_event_loop()
            futures = [ loop.run_in_executor(
                executor,
                functools.partial(self.LabeldQ, team=self.smurfs[idx], indices=indices, q=query)
            ) for idx, query in enumerate(self.queries) ]

        i = 0
        for resp in await asyncio.gather(*futures):
            df = esRest2Pandas(resp[1], self.agg1, self.agg2, self.aggs)
            self.redis.set(resp[0], df.to_msgpack(compress="zlib"))
            i += 1

        print("got", i+1, "responses")

    def LabeldQ(self, team, indices, q):
        return [ team, self.es.search(index=indices, body=q) ]

    def createQueries(self):
        try:
            q = json.load(open(self.qTpl))
        except Exception as e:
            raise e

        print(q)
        q["query"]["bool"]["must"].append(None)

        for i in range(1, self.workers+1):
            smurf = str(i).zfill(2)
            tq = copy.deepcopy(q)
            tq["query"]["bool"]["must"][1] = self.qString(smurf)
            self.queries.append(tq)
            self.smurfs.append(smurf)

    def qString(self, team):
        return  {
                "query_string": {
                    "default_field": "srcGEO",
                    "query": "srcGEO: %s AND dstGEO: %s" % (team, team)
                    }
            }

    def indexList(self):
        return ",".join(sorted([ *self.es.indices.get(self.indexPattern) ])[-self.limit:]  )

if __name__ == "__main__":
    hosts = ["host1:9200", "host2:9200"]
    party   =   SmurfParty(es_hosts=hosts)
    while True:
        party.BeerRun()
        time.sleep(60)

    #for team in party.GetData(): print(team)
