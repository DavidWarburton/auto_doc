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


class OnReservePat(TrackedTable):
    name = 'on_reserve_pat'
    index_from_year = {year: i for i, year in enumerate(range(1989, 2020))}

    def build(self, cursor=None):
        if not cursor:
            cursor = self.get_cursor()
        cursor.execute(
            "CREATE TABLE on_reserve_pat AS "
            "SELECT "
                "study_id, "
                "demographics_unduped_dob, "
                "'' AS on_reserve_pat_education_pattern, "
                "'' AS on_reserve_pat_residence_pattern, "
                "'' AS on_reserve_pat_synthesis_pattern "
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
            "SELECT "
                "study_id, "
                "import_geog_year "
            "FROM on_reserve_pat, all_years"
        )

        cursor.execute(
            "CREATE TEMPORARY TABLE on_reserve_pat_fill_in AS "
            "SELECT "
                "CASE "
                    "WHEN import_geog_postal_code_hash = ANY ("
                        "SELECT import_geog_postal_code_hash "
                        "FROM reserve_postal_codes_from_students "
                        "GROUP BY import_geog_postal_code_hash"
                    ") THEN '1'"
                    "ELSE '0' "
                "END AS on_reserve_education, "
                "CASE "
                    "WHEN import_geog_postal_code_hash = ANY ("
                        "SELECT import_geog_postal_code_hash "
                        "FROM reserve_postal_codes_from_aboriginal_residence "
                        "GROUP BY import_geog_postal_code_hash"
                    ") THEN '1'"
                    "ELSE '0' "
                "END AS on_reserve_residence, "
                "CASE "
                    "WHEN import_geog_postal_code_hash = ANY ("
                        "SELECT import_geog_postal_code_hash "
                        "FROM reserve_postal_codes_synthesis "
                        "GROUP BY import_geog_postal_code_hash"
                    ") THEN '1'"
                    "ELSE '0' "
                "END AS on_reserve_synthesis, "
                "a.import_geog_year, "
                "a.study_id "
            "FROM empty_years a LEFT JOIN geog_unduped b "
            "ON "
                "a.import_geog_year = b.import_geog_year AND "
                "a.study_id = b.study_id "
            "ORDER BY import_geog_year"
        )

        cursor.execute(
            "UPDATE on_reserve_pat a "
            "SET "
                "on_reserve_pat_education_pattern = string_agg_education_pattern, "
                "on_reserve_pat_residence_pattern = string_agg_residence_pattern "
            "FROM ("
                "SELECT "
                    "STRING_AGG("
                        "on_reserve_education, "
                        "''"
                    ") AS string_agg_education_pattern, "
                    "STRING_AGG("
                        "on_reserve_residence, "
                        "''"
                    ") AS string_agg_residence_pattern, "
                    "study_id "
                "FROM on_reserve_pat_fill_in "
                "GROUP BY study_id "
            ") AS sub_q "
            "WHERE a.study_id = sub_q.study_id "
        )
