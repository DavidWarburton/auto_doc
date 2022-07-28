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


class TestTable5(TrackedTable):
    name = 'test_table_5'

    def build(self, cursor=None):
        if not cursor:
            cursor = self.get_cursor()
        cursor.execute(
            "CREATE TABLE test_table_5 ("
                "test_table_5_column_1 NUMERIC, "
                "test_table_5_column_2 NUMERIC, "
                "test_table_3_column_1 NUMERIC "
            ")"
        )
        cursor.execute(
            "INSERT INTO test_table_5 (test_table_3_column_1) "
            "SELECT column_1 "
            "FROM test_table_3 "
        )

        cursor.execute(
            "UPDATE test_table_5 "
            "SET "
                "test_table_5_column_1 = t.i, "
                "test_table_5_column_2 = t2.i2 "
            "FROM generate_series(1, 100) as t(i), generate_series(1, 10) t2(i2) "
            "WHERE test_table_5_column_1 IS NULL AND test_table_5_column_2 IS NULL"
        )