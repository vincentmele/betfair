import logging

import betfairlightweight
from betfairlightweight import StreamListener

import psycopg2

import datetime

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

connection_str = "user=inplay host=127.0.0.1 port=5432 dbname=betfair" # password in ~/.pgpass

def get_race_ids(connection_str: str = None, event_id: int = 4339,
                start_time: datetime.datetime = None, end_time: datetime.datetime = None) -> List:
    # connect to postgresql and retrieve list of races to backtest
    table_name = 'inplay_history'

    if not connection_str:
        connection_str = "user=inplay host=127.0.0.1 port=5432 dbname=betfair" # password is in ~/.pgpass

    psql_connection = psycopg2.connect(dsn=connection_str)
    cursor = psql_connection.cursor()

    params = {}
    sql = f"SELECT DISTINCT id from {table_name} WHERE "

    if start_time and end_time:
        sql += "time > %(start_time)s and time < %(end_time)s AND "
        params['start_time'] = start_time
        params['end_time'] = end_time

    if event_id:
        sql += "cast(jsonb_path_query_first(data, '$.mc.marketDefinition.eventTypeId')->>0 as int4) = %(event_id)s " # 4339 is greyhounds, 7 is horses
        params['event_id'] = event_id

    sql += "order by id asc;"

    cursor.execute(sql, params)
    raceids = [r[0] for r in cursor.fetchall()]
    cursor.close()
    psql_connection.close()
    return raceids


race_ids = get_race_ids(start_time=datetime.datetime(2020, 11, 5), end_time=datetime.datetime(2020, 11, 7))

for id in race_ids:
    # create historical stream (update file_path to your file location)
    #query_str = "SELECT data::text from inplay_history WHERE time > DATE '2020-11-05' AND time < DATE '2020-11-08' ORDER BY time asc LIMIT 100000"
    print(f'STARTING RACE ID {id}')

    query_str = f"SELECT data::text from inplay_history WHERE id = {id} order by time asc"
    stream = trading.streaming.create_postgresql_historical_generator_stream(
        connection_str=connection_str, query_str=query_str, listener=listener,
    )

    # create generator
    gen = stream.get_generator()

    # print marketBooks
    #for market_books in gen():
    #    for market_book in market_books:
    #        print(market_book)

    # print based on seconds to start
    for market_books in gen():
        for market_book in market_books:
            seconds_to_start = (
                market_book.market_definition.market_time - market_book.publish_time
            ).total_seconds()
            if seconds_to_start < 100:
                print(market_book.market_id, seconds_to_start, market_book.total_matched)

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
