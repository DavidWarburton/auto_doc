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


class GraphAgeOverTime(TrackedGraph):
    name = 'graph_aboriginal_age_over_time'
    years = range(1989, 2018)
    age_ranges = [
        [0, 15],
        [16, 17],
        [18, 24],
        [25, 34],
        [35, 44],
        [45, 64],
        [65, 120],
    ]

    def build(self, cursor=None):
        if not cursor:
            cursor = self.get_cursor()

        create_or_insert_line = "CREATE TABLE graph_aboriginal_age_over_time AS"
        for year in self.years:
            sql = "SELECT {year} as year".format(year=year)
            for lower_age, higher_age in self.age_ranges:
                for population_name, population_sql in GraphAboriginalResidenceOverTime.populations.items():
                    conditions = (
                        "("
                            "AGE(DATE '{year}-07-01', demographics_unduped_dob) BETWEEN '{lower_age} YEARS' AND '{higher_age} YEARS 11 MONTHS 10 DAYS' AND "
                            "DATE '{year}-07-01' <= deaths_unduped_date_of_death"
                        ") AND "
                        "SUBSTRING("
                            "ses6_pat, "
                            "{ses_index}, "
                            "1"
                        ") != '0'".format(
                            year=year,
                            ses_index=Ses6.index_from_year_month["{year}-07".format(year=year)] + 1,
                            lower_age=lower_age,
                            higher_age=higher_age,
                        )
                    )
                    if population_sql:
                        conditions += " AND {population_sql}".format(population_sql=population_sql)

                    sql += (", "
                        "COUNT(study_id) "
                        "filter ("
                            "WHERE "
                                "{conditions}"
                        ") AS {name}_{lower_age}_{higher_age} ".format(
                            conditions=conditions,
                            name=population_name,
                            lower_age=lower_age,
                            higher_age=higher_age,
                        )
                    )
            sql += "FROM aboriginal_graph_maker"

            cursor.execute(
                "{create_or_insert_line} "
                "{sql}".format(
                    create_or_insert_line=create_or_insert_line,
                    sql=sql,
                )
            )
            create_or_insert_line = "INSERT INTO graph_aboriginal_age_over_time"


    def make_graph_product(self):
        df = self.load_dataframe()

        for lower_age, higher_age in self.age_ranges:
            age_range_str = "{lower_age}_{higher_age}".format(
                lower_age=lower_age,
                higher_age=higher_age,
            )

            # Sort columns so that the colors for each are persistent across graphs
            y_columns = sorted([
                column
                for column in df.columns
                if column.endswith(age_range_str) and
                (
                    column.startswith('broad') or
                    column.startswith('narrow')
                )
            ])
            df.plot(x='year', y=y_columns)
            self.save_plot("{}_over_time_aboriginal".format(age_range_str))

            # Sort columns so that the colors for each are persistent across graphs
            y_columns = sorted([
                column
                for column in df.columns
                if column.endswith(age_range_str) and
                (
                    column.startswith('non') or
                    column.startswith('full')
                )
            ])
            df.plot(x='year', y=y_columns)
            self.save_plot("{}_over_time_non_aboriginal".format(age_range_str))
