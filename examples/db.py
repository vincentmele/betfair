import psycopg2
import datetime

from typing import List, Dict

import examples.utils


class DB:

    def __init__(self, connection_str: str = None):
        self.connection_str = connection_str if connection_str else None
        self.conn = None
        self.cursor = None
        self._connect()
        self.table_name = 'inplay_history'

    def _connect(self):
        if self.conn is None or not self._is_alive():
            try:
                self.conn = psycopg2.connect(self.connection_str)
                self.cursor = self.conn.cursor()
            except Exception as e:
                print('Error connecting to timescaledb postgres instance. ' + str(e))

    def _is_alive(self):
        try:
            with self.conn.cursor() as cur:
                cur.execute('SELECT 1')
            return True
        except psycopg2.OperationalError:
            return False

    def insert_data(self, table):
        return NotImplemented

    def insert_checkpoint(self, race_id: int, milliseconds_to_start: int, data: str):
        params = {}
        cur = self.conn.cursor()
        sql = "INSERT INTO snapshots (id, milliseconds_to_start, data) VALUES (%(id)s, %(milliseconds_to_start)s, %(data)s) ON CONFLICT DO NOTHING"
        params['id'] = race_id
        params['milliseconds_to_start'] = milliseconds_to_start
        params['data'] = data
        cur.execute(sql, params)
        self.conn.commit()

    def get_race_ids(self, event_id: int = 4339,
                     start_time: datetime.datetime = None, end_time: datetime.datetime = None
                     ) -> List:

        params = {}
        sql = f"SELECT DISTINCT id from {self.table_name} WHERE "

        if start_time and end_time:
            sql += "time > %(start_time)s and time < %(end_time)s AND "
            params['start_time'] = start_time
            params['end_time'] = end_time

        if event_id:
            sql += "cast(jsonb_path_query_first(data, '$.mc.marketDefinition.eventTypeId')->>0 as int4) = %(event_id)s "  # 4339 is greyhounds, 7 is horses
            params['event_id'] = event_id

        sql += "order by id asc;"

        self.cursor.execute(sql, params)
        races = self.cursor.fetchall()
        self.cursor.close()

        return [r[0] for r in races]
