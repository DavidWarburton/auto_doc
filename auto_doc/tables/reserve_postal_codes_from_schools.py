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


class ReservePostalCodesFromSchools(TrackedTable):
    name = 'reserve_postal_codes_from_schools'

    def build(self, cursor=None):
        if not cursor:
            cursor = self.get_cursor()

        cursor.execute(
            "CREATE TEMPORARY TABLE students_in_band_schools AS "
            "SELECT "
                "studyid, "
                "SUBSTRING(b.school_year, 1, 4)::int as year, "
                "band_residency_status "
            "FROM "
                "band_school a "
                "LEFT OUTER JOIN education_schlstud b "
                    "ON b.mincode_this_enrol LIKE '%' || a.mincode || '%'"
            "WHERE "
                "TRIM(band_residency_status) = 'NON BAND RESIDENT' " # only look at non band residents becuase we're comparing this to the codes you get for band residents
            "GROUP BY 1, 2"
        )

        cursor.execute(
            "CREATE TABLE reserve_postal_codes_from_schools AS "
            "SELECT "
                "b.import_geog_postal_code_hash, "
                "b.import_geog_fsa "
            "FROM "
                "students_in_band_schools a "
                "LEFT OUTER JOIN geog_unduped b "
                    "ON  a.studyid = b.study_id "
                    "AND a.year = b.import_geog_year "
            "GROUP BY 1, 2"
        )