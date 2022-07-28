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


class GeogPat(TrackedTable):
    name = 'geog_pat'
    index_from_year = {year: i for i, year in enumerate(range(1989, 2020))}
    first_year_with_data = 2009

    def build(self, cursor=None):
        if not cursor:
            cursor = self.get_cursor()
        cursor.execute(
            "CREATE TABLE geog_pat AS "
            "SELECT "
                "study_id, "
                "demographics_unduped_dob, "
                "'' AS geog_pat_pop_ctr_ra_class_pattern "
            "FROM demographics_unduped"
        )

        cursor.execute(
            "CREATE TEMPORARY TABLE all_years AS "
            "SELECT import_geog_year "
            "FROM geog_unduped "
            "GROUP BY import_geog_year"
        )

        cursor.execute(
            "CREATE TEMPORARY TABLE empty_years AS "
            "SELECT study_id, import_geog_year "
            "FROM geog_pat, all_years"
        )

        cursor.execute(
            "CREATE TEMPORARY TABLE geog_pat_fill_in AS "
            "SELECT "
                "geog_unduped_pop_ctr_ra_class::TEXT AS geog_unduped_pop_ctr_ra_class, "
                "a.import_geog_year, "
                "a.study_id "
            "FROM empty_years a LEFT JOIN geog_unduped b "
            "ON "
                "a.import_geog_year = b.import_geog_year AND "
                "a.study_id = b.study_id "
            "ORDER BY import_geog_year"
        )

        cursor.execute(
            "UPDATE geog_pat a "
            "SET "
                "geog_pat_pop_ctr_ra_class_pattern = string_agg_pattern "
            "FROM ("
                "SELECT "
                    "STRING_AGG("
                        "geog_unduped_pop_ctr_ra_class, "
                        "''"
                    ") AS string_agg_pattern, "
                    "study_id "
                "FROM geog_pat_fill_in "
                "GROUP BY study_id "
            ") AS sub_q "
            "WHERE a.study_id = sub_q.study_id "
        )
