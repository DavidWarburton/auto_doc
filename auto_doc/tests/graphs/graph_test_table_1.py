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

from ...auto_doc import TrackedGraph


class GraphTestTable1(TrackedGraph):
    name = 'graph_test_table_1'

    def build(self, cursor=None):
        if not cursor:
            cursor = self.get_cursor()
        cursor.execute(
            "CREATE TABLE graph_test_table_1 ("
                "graph_test_table_1_column_1 NUMERIC, "
                "graph_test_table_1_column_2 NUMERIC "
            ")"
        )

        cursor.execute("INSERT INTO graph_test_table_1 VALUES (1, 1)")
        cursor.execute("INSERT INTO graph_test_table_1 VALUES (2, 2)")
        cursor.execute("INSERT INTO graph_test_table_1 VALUES (3, 3)")
        cursor.execute("INSERT INTO graph_test_table_1 VALUES (4, 4)")
        cursor.execute("INSERT INTO graph_test_table_1 VALUES (5, 5)")

    def make_graph_product(self):
        pass
