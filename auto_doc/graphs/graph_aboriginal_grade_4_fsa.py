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


class GraphAboriginalGrade4Fsa(TrackedGraph):
    name = 'graph_aboriginal_grade_4_fsa'
    fsa_categories = ['reading', 'writing', 'numeracy']

    def build(self, cursor=None):
        if not cursor:
            cursor = self.get_cursor()

        create_str = (
            "CREATE TABLE graph_aboriginal_grade_4_fsa AS "
            "SELECT "
                "COUNT(*), "
        )

        for category in self.fsa_categories:
            create_str += (
                "COUNT(*) filter ("
                    "WHERE "
                        "aboriginal_graph_maker_grd4_{category}_fsa_code <= 3"
                ") AS count_meet_or_exceed_g4_{category}, "
                "COUNT(*) filter ("
                    "WHERE "
                        "TRIM(education_fsa_short_not_excused_participant) = '0'"
                ") AS count_not_writing_g4_{category}, ".format(category=category)
            )

        create_str += (
            "aboriginal_status_in_health, "
            "(aboriginal_status_in_health OR aboriginal_status_in_education) as aboriginal_status_in_broad, "
            "education_smaller_yr_band_resident, "
            "education_fsa_short_school_year "
            "FROM aboriginal_graph_maker "
            "WHERE TRIM(education_fsa_short_excused) = '0'"
            "GROUP BY "
                "aboriginal_status_in_health, "
                "aboriginal_status_in_broad, "
                "education_smaller_yr_band_resident, "
                "education_fsa_short_school_year"
        )
        cursor.execute(create_str)

    def make_graph_product(self):
        df = self.load_dataframe()
