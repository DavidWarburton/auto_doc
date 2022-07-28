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


class GraphAboriginalGrade10(TrackedGraph):
    name = 'graph_aboriginal_grade_10'

    def build(self, cursor=None):
        if not cursor:
            cursor = self.get_cursor()

        cursor.execute(
            "SELECT education_gr10_code "
            "FROM education_gr10 "
            "GROUP BY education_gr10_code"
        )
        courses = [tup[0].strip() for tup in cursor.fetchall() if tup[0].strip()]

        create_str = (
            "CREATE TABLE graph_aboriginal_grade_10 AS "
            "SELECT "
                "COUNT(*), "
        )

        for course in courses:
            create_str += (
                "AVG(education_gr10_schlstud_{course}_final) as average_g10_{course}, "
                "COUNT(*) filter ("
                    "WHERE "
                        "education_gr10_schlstud_{course}_final IS NULL"
                ") AS count_not_writing_g10_{course}_final, ".format(course=course)
            )

        create_str += (
            "aboriginal_status_in_health, "
            "(aboriginal_status_in_health OR aboriginal_status_in_education) as aboriginal_status_in_broad, "
            "education_gr10_schlstud_band_residency_status, "
            "education_gr10_schlstud_school_year "
            "FROM education_gr10_schlstud "
            "GROUP BY "
                "aboriginal_status_in_health, "
                "aboriginal_status_in_broad, "
                "education_gr10_schlstud_band_residency_status, "
                "education_gr10_schlstud_school_year"
        )
        cursor.execute(create_str)

    def make_graph_product(self):
        df = self.load_dataframe()
