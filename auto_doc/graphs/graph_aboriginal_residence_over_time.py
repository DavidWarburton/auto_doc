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
from ..tables.ses6 import Ses6
from ..tables.geog_pat import GeogPat

class GraphAboriginalResidenceOverTime(TrackedGraph):
    name = 'graph_aboriginal_residence_over_time'
    
    populations = {
        'narrow': "aboriginal_status_in_health = True",
        'broad': "(aboriginal_status_in_health = True OR aboriginal_status_in_education = True)",
        'non_narrow': "aboriginal_status_in_health = False",
        'non_broad': "(aboriginal_status_in_health = False AND aboriginal_status_in_education = False)",
        'full_population': "",
    }

    def build(self, cursor=None):
        if not cursor:
            cursor = self.get_cursor()

        cursor.execute(
            "SELECT geog_pat_pop_ctr_ra_class_pattern "
            "FROM aboriginal_graph_maker "
            "GROUP BY geog_pat_pop_ctr_ra_class_pattern"
        )
        pop_ctr_ra_class_values = set()
        for geog_pat_tuple in cursor.fetchall():
            for geog_class in geog_pat_tuple[0]:
                pop_ctr_ra_class_values.add(geog_class)

        cursor.execute(
            "SELECT import_geog_year "
            "FROM import_geog "
            "GROUP BY import_geog_year"
        )

        create_or_insert_line = "CREATE TABLE graph_aboriginal_residence_over_time AS"
        years = [year for year in GeogPat.index_from_year.keys() if year <= Ses6.last_year]
        for year in years:
            sql = "SELECT {year} as year".format(year=year)
            for pop_ctr_value in pop_ctr_ra_class_values:
                for population_name, population_sql in self.populations.items():

                    conditions = (
                        "SUBSTRING("
                            "geog_pat_pop_ctr_ra_class_pattern, "
                            "{geog_pat_index}, "
                            "1"
                        ") = '{pop_ctr_value}' AND " # Pop_ctr_ra_class matches current value
                        "SUBSTRING("
                            "ses6_pat, "
                            "{ses6_pat_index}, "
                            "1"
                        ") != '0' AND " # is in BC
                        "DATE '{year}-07-01' <= deaths_unduped_date_of_death AND " # Is not dead
                        "DATE '{year}-07-01' >= demographics_unduped_dob AND " # Is born
                        "AGE(DATE '{year}-07-01', demographics_unduped_dob) <= '120 YEARS 11 MONTHS 10 DAYS'" # Is less than 120 years old (because people don't actualy get older than this, so if you look like you're this old it's because your death wasn't recorded)
                        "".format(
                            geog_pat_index=GeogPat.index_from_year[year] + 1,
                            ses6_pat_index=Ses6.index_from_year_month["{year}-07".format(year=year)] + 1,
                            pop_ctr_value=pop_ctr_value,
                            year=year,
                        )
                    )
                    if population_sql:
                        conditions += " AND {population_sql}".format(population_sql=population_sql)

                    sql += (", "
                        "COUNT(study_id) "
                        "filter ("
                            "WHERE "
                                "{conditions}"
                        ") AS {name}_{pop_ctr_value} ".format(
                            name=population_name,
                            pop_ctr_value=pop_ctr_value,
                            conditions=conditions,
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
            create_or_insert_line = "INSERT INTO graph_aboriginal_residence_over_time"

    def make_graph_product(self):
        if not cursor:
            cursor = self.get_cursor()

        df = self.load_dataframe(index_col='year')
        df = df.truncate(before=GeogPat.first_year_with_data, copy=False)

        pop_ctr_ra_class_values = set(col_name[-1] for col_name in df.columns)

        for pop_ctr_value in pop_ctr_ra_class_values:
            # Sort columns so that the colors for each are persistent across graphs
            y_columns = sorted([
                column
                for column in df.columns
                if column.endswith(pop_ctr_value) and
                (
                    column.startswith('broad') or
                    column.startswith('narrow')
                )
            ])
            df.plot(y=y_columns)
            self.save_plot("{}_over_time_aboriginal".format(pop_ctr_value))

            # Sort columns so that the colors for each are persistent across graphs
            y_columns = sorted([
                column
                for column in df.columns
                if column.endswith(pop_ctr_value) and
                (
                    column.startswith('non') or
                    column.startswith('full')
                )
            ])
            df.plot(y=y_columns)
            self.save_plot("{}_over_time_non_aboriginal".format(pop_ctr_value))
