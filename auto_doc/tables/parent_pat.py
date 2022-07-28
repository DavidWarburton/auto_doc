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


class ParentPat(TrackedTable):
    name = 'parent_pat'

    def build(self, cursor=None):
        if not cursor:
            cursor = self.get_cursor()

        cursor.execute(
            "CREATE TABLE parent_pat AS "
            "SELECT "
                "study_id, "
                "demographics_unduped_dob, "
                "'{blank_pattern}' AS parent_pat_pattern "
            "FROM demographics_unduped "
            "LIMIT 100".format(
                blank_pattern="0"*12*19,
            )
        )

        cursor.execute(
            "UPDATE parent_pat a "
            "SET "
                "parent_pat_pattern = "
                    "substring("
                        "parent_pat_pattern, "
                        "0, "
                        "((DATE_PART("
                            "'year', "
                            "b.bounded_parenthood_end_date"
                        ") - {start_year})*12 + DATE_PART("
                            "'month', "
                            "b.bounded_parenthood_end_date"
                        "))::int"
                    ") || "
                    "substring("
                        "'{parenthood_str}', "
                        "0, "
                        "((DATE_PART("
                            "'year', "
                            "b.bounded_parenthood_end_date"
                        ") - {start_year})*12 + DATE_PART("
                            "'month', "
                            "b.bounded_parenthood_end_date"
                        ") - (DATE_PART("
                            "'year', "
                            "b.bounded_parenthood_start_date"
                        ") - {start_year})*12 + DATE_PART("
                            "'month', "
                            "b.bounded_parenthood_start_date"
                        "))::int + 1"
                    ") || "
                    "substring("
                        "parent_pat_pattern, "
                        "((DATE_PART("
                            "'year', "
                            "b.bounded_parenthood_end_date"
                        ") - {start_year})*12 + DATE_PART("
                            "'month', "
                            "b.bounded_parenthood_end_date"
                        "))::int + 1"
                    ")"
            "FROM bounded_parenthood b "
            "WHERE a.study_id = b.bounded_parenthood_parent_study_id_1 OR "
                "a.study_id = b.bounded_parenthood_parent_study_id_2".format(
                parenthood_str="1"*12*19,
                start_year='1985',
            )
        )
