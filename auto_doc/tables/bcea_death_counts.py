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

from ..auto_doc import TrackedTable


class BceaDeathCounts(TrackedTable):
    name = 'bcea_death_counts'

    def build(self, cursor=None):
        if not cursor:
            cursor = self.get_cursor()
        
        cursor.execute("""
            CREATE TABLE bcea_death_counts AS
            SELECT 
                bcea_oop_year_month, COUNT(*) AS case_load,
                0 AS nfa_case_load,
                0 AS total_deaths,
                0 AS nfa_deaths
            FROM bcea_involv_w_deaths
            GROUP BY bcea_oop_year_month
            ORDER BY bcea_oop_year_month
        """)
        
        cursor.execute("""
            UPDATE bcea_death_counts a
            SET nfa_case_load = sub_q.nfa_case_load
            FROM (
                SELECT bcea_oop_year_month, COUNT(*) AS nfa_case_load
                FROM bcea_involv_w_deaths
                WHERE bcea_involv_w_deaths_no_fixed_address = true
                GROUP BY bcea_oop_year_month
            ) AS sub_q
            WHERE a.bcea_oop_year_month = sub_q.bcea_oop_year_month
        """)

        cursor.execute("""
            UPDATE bcea_death_counts a
            SET total_deaths = sub_q.total_deaths
            FROM (
                SELECT bcea_oop_year_month, COUNT(*) AS total_deaths
                FROM bcea_involv_w_deaths
                WHERE bcea_involv_w_deaths_died_within_12_months = true
                GROUP BY bcea_oop_year_month
            ) AS sub_q
            WHERE a.bcea_oop_year_month = sub_q.bcea_oop_year_month
        """)


        cursor.execute("""
            UPDATE bcea_death_counts a
            SET nfa_deaths = sub_q.nfa_death_count
            FROM (
                SELECT bcea_oop_year_month, COUNT(*) AS nfa_death_count
                FROM bcea_involv_w_deaths
                WHERE bcea_involv_w_deaths_no_fixed_address = true AND bcea_involv_w_deaths_died_within_12_months = true
                GROUP BY bcea_oop_year_month
            ) AS sub_q
            WHERE a.bcea_oop_year_month = sub_q.bcea_oop_year_month
        """)
