import logging

import betfairlightweight
from betfairlightweight import StreamListener
from betfairlightweight.resources.bettingresources import MarketBook, RunnerBook

import psycopg2
import msgpack
import ujson as json

import examples.db

from datetime import timedelta, datetime

from typing import List, Dict

"""
Data needs to be downloaded from:
    https://historicdata.betfair.com
"""

# setup logging
logging.basicConfig(level=logging.INFO)

# create trading instance (don't need username/password)
trading = betfairlightweight.APIClient("username", "password", app_key='')

# create listener
listener = StreamListener(max_latency=None)

def find_favorites(market_book: MarketBook, number: int = 1) -> List[RunnerBook]:
    sorted_list = sorted(market_book.runners,
                         key=lambda x: x.ex.available_to_back[0].price if len(x.ex.available_to_back) > 0 else 1000)
    return sorted_list[:number]


db = examples.db.DB()

race_ids = db.get_race_ids(start_time=datetime(2020, 11, 12), end_time=datetime(2020, 11, 12, 23, 59))

for id in race_ids:
    # create historical stream (update file_path to your file location)
    #query_str = "SELECT data::text from inplay_history WHERE time > DATE '2020-11-05' AND time < DATE '2020-11-08' ORDER BY time asc LIMIT 100000"
    print(f'STARTING RACE ID {id}')

    query_str = f"SELECT data::text from inplay_history WHERE id = {id} order by time asc"
    stream = trading.streaming.create_postgresql_historical_generator_stream(
        connection_str=db.connection_str, query_str=query_str, listener=listener,
    )

    # create generator
    gen = stream.get_generator()

    # print marketBooks
    #for market_books in gen():
    #    for market_book in market_books:
    #        print(market_book)

    def snapshot(_db, _market_book, _seconds_to_start):
        _raceid = int(_market_book.market_id.replace('1.1', '11'))
        _milliseconds_to_start = _seconds_to_start * 1000
        _d = market_book.json()
        _db.insert_checkpoint(_raceid, _milliseconds_to_start, _d)

    snapshotted = {1800: None, 600: None, 300: None, 180: None, 120: None, 60: None, 30: None, 15: None, 10: None, 5: None}
    # print based on seconds to start
    for market_books in gen():
        for market_book in market_books:
            seconds_to_start = (
                market_book.market_definition.market_time - market_book.publish_time
            ).total_seconds()
            for k in snapshotted.keys():
                if seconds_to_start < k and not snapshotted[k]:
                    snapshot(db, market_book, k)
                    snapshotted[k] = True

           # if seconds_to_start < 120 and not snapshotted['120']:
          #      snapshot(db, market_book, seconds_to_start)
                #abcde = msgpack.packb(json.loads(market_book.json()))
                #packed = pack(abcde)
                #json_abcde = market_book.json()
            #    abc = find_favorites(market_book)
           #     print(market_book.market_id, seconds_to_start, market_book.total_matched)


            # print winner details once market is closed
            if market_book.status == "CLOSED":
                for runner in market_book.runners:
                    if runner.status == "WINNER":
                        print(
                            "{0}: {1} with sp of {2}".format(
                                runner.status, runner.selection_id, runner.sp.actual_sp
                            )
                        )

    # record prices to a file
#    with open("output.txt", "w") as output:
#        output.write("Time,MarketId,Status,Inplay,SelectionId,LastPriceTraded\n")

#    for market_books in gen():
#        for market_book in market_books:
#            with open("output.txt", "a") as output:
#                for runner in market_book.runners:
#                    # how to get runner details from the market definition
#                    market_def = market_book.market_definition
#                    runners_dict = {
#                        (runner.selection_id, runner.handicap): runner
#                        for runner in market_def.runners
#                    }
#                    runner_def = runners_dict.get((runner.selection_id, runner.handicap))

#                    output.write(
#                        "%s,%s,%s,%s,%s,%s\n"
#                        % (
#                            market_book.publish_time,
#                            market_book.market_id,
 #                           market_book.status,
#                           market_book.inplay,
#                            runner.selection_id,
#                            runner.last_price_traded or "",
#                       )
#                   )
