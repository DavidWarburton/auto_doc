"""
Copyright 2019 Province of British Columbia

Licensed under the Apache License, Version 2.0 the "License";
you may not use this file except in compliance with the License.
You may obtain a copy of the License at 

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""

from ...auto_doc import TrackedTable

from decimal import Decimal
from collections import OrderedDict
from psycopg2.extras import NumericRange, DateRange, DateTimeRange, DateTimeTZRange, register_uuid, register_hstore
from uuid import UUID
from ipaddress import ip_network
from datetime import date, time, datetime, timedelta
import pytz, json

class TestTableTypes(TrackedTable):
    name = 'test_table_types'
    default_values = OrderedDict([
        ('test_table_types_bool', True),
        ('test_table_types_real', 9.99),
        ('test_table_types_double', 9.99),
        ('test_table_types_smallint', 9),
        ('test_table_types_integer', 50000),
        ('test_table_types_bigint', 5000000000),
        ('test_table_types_numeric', Decimal('1.1')),
        ('test_table_types_varchar', 'hi there'),
        ('test_table_types_text', 'hi there'),
        ('test_table_types_bytea', b'hi there'),
        ('test_table_types_date', date(year=1991, month=11, day=11)),
        ('test_table_types_time', time(hour=11, minute=39, second=22)),
        ('test_table_types_timetz', time(hour=11, minute=39, second=22, tzinfo=pytz.utc)),
        ('test_table_types_timestamp', datetime(year=1991, month=11, day=11, hour=11, minute=39, second=22)),
        ('test_table_types_timestamptz', datetime(year=1991, month=11, day=11, hour=11, minute=39, second=22, tzinfo=pytz.utc)),
        ('test_table_types_interval', timedelta(hours=15)),
        ('test_table_types_array', ['a', 'b', 'c', 'd', 'e']),
        # ('test_table_types_hstore', {'a': 1, 'b': 2, 'c': 3}), this dict would be passed to test_table_types_hstore
        ('test_table_types_int4range', NumericRange(lower=1, upper=10)),
        ('test_table_types_int8range', NumericRange(lower=1, upper=50000)),
        ('test_table_types_numrange', NumericRange(lower=Decimal('0.1'), upper=Decimal('1.1'))),
        ('test_table_types_daterange', DateRange(
            lower=date(year=1991, month=11, day=11),
            upper=date(year=1991, month=11, day=21),
        )),
        ('test_table_types_tsrange', DateTimeRange(
            lower=datetime(year=1991, month=11, day=11, hour=11, minute=39, second=22),
            upper=datetime(year=1991, month=11, day=21, hour=11, minute=39, second=22),
        )),
        ('test_table_types_tstzrange', DateTimeTZRange(
            lower=datetime(year=1991, month=11, day=11, hour=11, minute=39, second=22, tzinfo=pytz.utc),
            upper=datetime(year=1991, month=11, day=21, hour=11, minute=39, second=22, tzinfo=pytz.utc),
        )),
        # ('test_table_types_uuid', UUID(bytes=b'1234567890123456')),
        # ('test_table_types_inet', ip_network('192.168.0.0')),
        # ('test_table_types_cidr', ip_network('192.168.0.0')),
        ('test_table_types_json', json.dumps(['a', {'b': 2}, 2])),
        ('test_table_types_jsonb', json.dumps(['a', {'b': 2}, 2])),
    ])

    def build(self, cursor=None):
        if not cursor:
            cursor = self.get_cursor()

        cursor.execute(
            "CREATE TABLE test_table_types ("
                "test_table_types_bool bool DEFAULT %s, "
                "test_table_types_real real DEFAULT %s, "
                "test_table_types_double double precision DEFAULT %s, "
                "test_table_types_smallint smallint DEFAULT %s, "
                "test_table_types_integer integer DEFAULT %s, "
                "test_table_types_bigint bigint DEFAULT %s, "
                "test_table_types_numeric numeric DEFAULT %s, "
                "test_table_types_varchar varchar DEFAULT %s, "
                "test_table_types_text text DEFAULT %s, "
                "test_table_types_bytea bytea DEFAULT %s, "
                "test_table_types_date date DEFAULT %s, "
                "test_table_types_time time DEFAULT %s, "
                "test_table_types_timetz timetz DEFAULT %s, "
                "test_table_types_timestamp timestamp DEFAULT %s, "
                "test_table_types_timestamptz timestamptz DEFAULT %s, "
                "test_table_types_interval interval DEFAULT %s, "
                "test_table_types_array text[] DEFAULT %s, "
                # "test_table_types_hstore hstore DEFAULT %s, " hstore is unused.
                "test_table_types_int4range int4range DEFAULT %s, "
                "test_table_types_int8range int8range DEFAULT %s, "
                "test_table_types_numrange numrange DEFAULT %s, "
                "test_table_types_daterange daterange DEFAULT %s, "
                "test_table_types_tsrange tsrange DEFAULT %s, "
                "test_table_types_tstzrange tstzrange DEFAULT %s, "
                # "test_table_types_uuid uuid DEFAULT %s, " uuid is unused.
                # "test_table_types_inet inet DEFAULT %s, " inet types are unused.
                # "test_table_types_cidr cidr DEFAULT %s, "
                "test_table_types_json json DEFAULT %s, "
                "test_table_types_jsonb jsonb DEFAULT %s "
            ")", list(self.default_values.values())
        )

        for i in range(1000):
            cursor.execute("INSERT INTO test_table_types DEFAULT VALUES")