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
from datetime import date
from dateutil import relativedelta


class IaPop(TrackedTable):
    name = 'ia_pop_2001_2002'

    def build(self, cursor=None):
        if not cursor:
            cursor = self.get_cursor()

        tracked_tables = TrackedTable.get_all()

        first_month = tracked_tables['ia_pat_all'].first_month

        years = [date(year=2001, month=3, day=15), date(year=2002, month=3, day=15)]

        print("create table")
        cursor.execute("""
            CREATE TABLE ia_pop_2001_2002 (
                studyid TEXT,
                dobyyyymm DATE,
                sex TEXT,
                on_ia_march BOOLEAN,
                count_ia_last_year INT,
                count_ia_two_years_ago INT,
                year INT,
                fill_in BOOLEAN
            )
        """)

        for year in years:
            rel_del = relativedelta.relativedelta(year, first_month)
            march_of_year_as_int = rel_del.months + 12 * rel_del.years

            print("base pop for {year}".format(year=year.year))
            cursor.execute("""
                DROP TABLE IF EXISTS ia_pop_{year};
                CREATE TEMPORARY TABLE ia_pop_{year} AS
                SELECT
                    COALESCE(a.study_id, b.study_id) AS studyid,
                    a.demographics_unduped_dob AS dobyyyymm,
                    a.demographics_unduped_gender AS sex,
                    (CASE WHEN SUBSTRING(b.ia_pat_all_pattern FROM {march_of_year_as_int} FOR 1) = '1' THEN true ELSE false END)AS on_ia_march,
                    LENGTH(REPLACE(SUBSTRING(b.ia_pat_all_pattern FROM {last_apr_as_int} FOR 11), '0', '')) AS count_ia_last_year,
                    LENGTH(REPLACE(SUBSTRING(b.ia_pat_all_pattern FROM {two_mars_ago_as_int} FOR 12), '0', '')) AS count_ia_two_years_ago
                FROM demographics_unduped a LEFT OUTER JOIN ia_pat_all b ON a.study_id = b.study_id;
            """.format(
                    year=year.year,
                    march_of_year_as_int=march_of_year_as_int,
                    last_apr_as_int=march_of_year_as_int-11,
                    two_mars_ago_as_int=march_of_year_as_int-23,
                )
            )

            print("delete not in r&pb {year}".format(year=year.year))
            cursor.execute("""
                DELETE FROM ia_pop_{year} a
                WHERE NOT EXISTS (
                    SELECT FROM rpb_contract_fix_cancel b
                    WHERE a.studyid = b.study_id AND TO_DATE('{formated_year}', 'YYYY-MM-DD') BETWEEN b.rpb_contract_fix_cancel_start_date AND b.rpb_contract_fix_cancel_cancel_date
                )
            """.format(
                    year=year.year,
                    formated_year=year.strftime("%Y-%m-%d"),
                )
            )

            print("fill in base for {year}".format(year=year.year))
            cursor.execute("""
                CREATE TEMPORARY TABLE ia_pop_fill_in_{year} AS
                SELECT
                    COALESCE(a.study_id, b.study_id) AS studyid,
                    a.demographics_unduped_dob AS dobyyyymm,
                    a.demographics_unduped_gender AS sex,
                    (CASE WHEN SUBSTRING(b.ia_pat_all_pattern FROM {march_of_year_as_int} FOR 1) = '1' THEN true ELSE false END)AS on_ia_march,
                    LENGTH(REPLACE(SUBSTRING(b.ia_pat_all_pattern FROM {last_apr_as_int} FOR 11), '0', '')) AS count_ia_last_year,
                    LENGTH(REPLACE(SUBSTRING(b.ia_pat_all_pattern FROM {two_mars_ago_as_int} FOR 12), '0', '')) AS count_ia_two_years_ago
                FROM demographics_unduped a FULL OUTER JOIN ia_pat_all b ON a.study_id = b.study_id
                WHERE EXTRACT(year FROM a.demographics_unduped_dob) BETWEEN {twenty_three_years_ago} AND {nineteen_years_ago};
            """.format(
                    year=year.year,
                    march_of_year_as_int=march_of_year_as_int,
                    last_apr_as_int=march_of_year_as_int-11,
                    two_mars_ago_as_int=march_of_year_as_int-23,
                    nineteen_years_ago=year.year-19,
                    twenty_three_years_ago=year.year-23,
                )
            )

            print("delete already found for {year}".format(year=year.year))
            cursor.execute("""
                DELETE FROM ia_pop_fill_in_{year} a
                WHERE EXISTS (
                    SELECT FROM ia_pop_{year} b
                    WHERE a.studyid = b.studyid
                )
            """.format(
                    year=year.year,
                )
            )

            print("delete not in r&pb {year}".format(year=year.year))
            cursor.execute("""
                DELETE FROM ia_pop_fill_in_{year} a
                WHERE NOT EXISTS (
                    SELECT FROM rpb_contract_fix_cancel b
                    WHERE a.studyid = b.study_id AND a.dobyyyymm + INTERVAL '19 years' BETWEEN b.rpb_contract_fix_cancel_start_date AND b.rpb_contract_fix_cancel_cancel_date
                )
            """.format(
                    year=year.year,
                )
            )

            print("insert {year} into table".format(year=year.year))
            cursor.execute("""
                INSERT INTO ia_pop_2001_2002
                SELECT
                    studyid,
                    dobyyyymm,
                    sex,
                    on_ia_march,
                    count_ia_last_year,
                    count_ia_two_years_ago,
                    {year} AS year,
                    false AS fill_in
                FROM ia_pop_{year}
            """.format(
                    year=year.year,
                )
            )

            print("insert fill in {year} into table".format(year=year.year))
            cursor.execute("""
                INSERT INTO ia_pop_2001_2002
                SELECT
                    studyid,
                    dobyyyymm,
                    sex,
                    on_ia_march,
                    count_ia_last_year,
                    count_ia_two_years_ago,
                    {year} AS year,
                    true AS fill_in
                FROM ia_pop_fill_in_{year}
            """.format(
                    year=year.year,
                )
            )
