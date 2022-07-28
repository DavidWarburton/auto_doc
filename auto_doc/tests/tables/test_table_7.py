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


class TestTable7(TrackedTable):
    name = 'test_table_7'

    def build(self, cursor=None):
        if not cursor:
            cursor = self.get_cursor()
        cursor.execute(
            "CREATE TABLE test_table_7 ("
                "column_1_7 NUMERIC"
            ")"
        )


        cursor.execute(
            "INSERT INTO test_table_7 "
            "SELECT "
                "5 "
            "FROM generate_series(1, 1000)"
        )