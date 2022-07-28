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


class ReservePostalCodesFromAboriginalResidence(TrackedTable):
    name = 'reserve_postal_codes_from_aboriginal_residence'

    def build(self, cursor=None):
        if not cursor:
            cursor = self.get_cursor()

        cursor.execute(
            "CREATE TEMPORARY TABLE residence_counts_by_aboriginal_status AS "
            "SELECT "
                "COUNT(*) FILTER (WHERE aboriginal_status_in_any = TRUE) AS aboriginal_residents, "
                "COUNT(*) FILTER (WHERE aboriginal_status_in_any = FALSE) AS non_aboriginal_residents, "
                "import_geog_postal_code_hash "
            "FROM "
                "geog_unduped a "
                "LEFT OUTER JOIN aboriginal_status b "
                    "ON a.study_id = b.study_id "
            "GROUP BY import_geog_postal_code_hash"
        )

        cursor.execute(
            "CREATE TABLE reserve_postal_codes_from_aboriginal_residence AS "
            "SELECT "
                "import_geog_postal_code_hash "
            "FROM residence_counts_by_aboriginal_status "
            "WHERE aboriginal_residents > non_aboriginal_residents"
        )
