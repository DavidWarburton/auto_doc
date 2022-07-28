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
from .graph_aboriginal_residence_over_time import GraphAboriginalResidenceOverTime
from ..tables.ses6 import Ses6


class GraphAboriginalResidenceOverTimeByGraduation(TrackedGraph):
    name = 'graph_aboriginal_graduation'

    def build(self, cursor=None):
        if not cursor:
            cursor = self.get_cursor()

        cursor.execute(
            "CREATE TABLE graph_aboriginal_graduation AS "
            "SELECT "
                "COUNT(*) as count, "
                "CASE "
                    "WHEN ("
                        "education_ever_grad_grad = '1' AND "
                        "TRUNC(education_ever_grad_grad_date) - education_highest_bc_enter_grd_8_year <= 6"
                    ") THEN 1 "
                    "WHEN ("
                        "education_adult_grad_agrad = '1' AND "
                        "TRUNC(education_adult_grad_agrad_date) - education_highest_bc_enter_grd_8_year <= 6"
                    ") THEN 2 "
                    "ELSE 0 "
                "END AS grad_status, "
                "aboriginal_status_in_health, "
                "(aboriginal_status_in_health OR aboriginal_status_in_education) as aboriginal_status_in_broad, "
                "education_gr10_schlstud_band_residency_status, "
                "education_highest_bc_enter_grd_8_year "
            "FROM education_highest_bc "
            "WHERE "
                "education_highest_bc_in_possible_grad_pop = TRUE AND "
                "TRIM(education_gr10_schlstud_bc_residency_status) != 'Non Resident' "
            "GROUP BY "
                "grad_status, "
                "aboriginal_status_in_health, "
                "aboriginal_status_in_broad, "
                "education_gr10_schlstud_band_residency_status, "
                "education_highest_bc_enter_grd_8_year"
        )

    def make_graph_product(self):
        df = self.load_dataframe()
        """
        for population in GraphAboriginalResidenceOverTime.populations.keys():
            # Sort columns so that the colors for each are persistent across graphs
            y_columns = [
                '{population}_{education_level}'.format(
                    population=population,
                    education_level=education_level,
                ) for education_level in self.education_levels.keys()
            ]
            df.plot(y=y_columns)
            self.save_plot("{}_graduation_over_time".format(population))
        """
