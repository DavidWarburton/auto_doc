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

from ..auto_doc import TrackedGraph
from itertools import product, combinations


class GraphAboriginalStatus(TrackedGraph):
    name = 'graph_aboriginal_status_by_birthyear'
    aboriginal_indicator_columns = [
        'in_any',
        'in_msp',
        'in_mcfd',
        'in_births',
        'in_education',
        'in_pssg',
        'in_deaths',
    ]

    def build(self, cursor=None):

        if not cursor:
            cursor = self.get_cursor()
        create_sql = (
            "CREATE TABLE graph_aboriginal_status_by_birthyear AS "
            "SELECT "
                "DATE_PART('year', demographics_unduped_dob) as year, "
                "COUNT(*) AS total_count"
        )

        for indicator in self.aboriginal_indicator_columns:
            create_sql += (", "
                "COUNT("
                    "CASE "
                        "WHEN aboriginal_status_{indicator} THEN 1 "
                    "END "
                ") AS aboriginal_status_by_birthyear_count_{indicator}".format(indicator=indicator)
            )

        create_sql += (" "
            "FROM aboriginal_status "
            "GROUP BY year"
        )
        cursor.execute(create_sql)

        for combo in combinations(self.aboriginal_indicator_columns, 2):
            cursor.execute(
                "ALTER TABLE graph_aboriginal_status_by_birthyear "
                "ADD COLUMN "
                    "{column_1}_{column_2}_true_true "
                "INTEGER, "
                "ADD COLUMN "
                    "{column_1}_{column_2}_false_false "
                "INTEGER, "
                "ADD COLUMN "
                    "{column_1}_{column_2}_false_true "
                "INTEGER, "
                "ADD COLUMN "
                    "{column_1}_{column_2}_true_false "
                "INTEGER".format(
                    column_1=combo[0],
                    column_2=combo[1],
                )
            )

            cursor.execute(
                "UPDATE graph_aboriginal_status_by_birthyear a "
                "SET "
                    "{column_1}_{column_2}_true_true = "
                        "true_true_count, "
                    "{column_1}_{column_2}_false_true = "
                        "false_true_count, "
                    "{column_1}_{column_2}_true_false = "
                        "true_false_count, "
                    "{column_1}_{column_2}_false_false = "
                        "false_false_count "
                "FROM ("
                    "SELECT "
                        "DATE_PART('year', demographics_unduped_dob) as year, "
                        "COUNT("
                            "CASE "
                                "WHEN aboriginal_status_{column_1} AND "
                                    "aboriginal_status_{column_2} THEN 1"
                            "END "
                        ") AS true_true_count, "
                        "COUNT("
                            "CASE "
                                "WHEN aboriginal_status_{column_1} = false AND "
                                    "aboriginal_status_{column_2} THEN 1"
                            "END "
                        ") AS false_true_count, "
                        "COUNT("
                            "CASE "
                                "WHEN aboriginal_status_{column_1} AND "
                                    "aboriginal_status_{column_2} = false THEN 1"
                            "END "
                        ") AS true_false_count, "
                        "COUNT("
                            "CASE "
                                "WHEN aboriginal_status_{column_1} = false AND "
                                    "aboriginal_status_{column_2} = false THEN 1"
                            "END "
                        ") AS false_false_count "
                    "FROM aboriginal_status "
                    "GROUP BY year "
                ") AS sub_q "
                "WHERE sub_q.year = a.year".format(
                    column_1=combo[0],
                    column_2=combo[1],
                )
            )

    def make_graph_product(self):
        raise
        df = self.load_dataframe()

        x_columns = set(['year'])
        y_columns = set(df.columns) - x_columns

        for x, y in product(x_columns, y_columns):
            df.plot(x=x, y=y)
            self.save_plot("{}_{}".format(x, y))
