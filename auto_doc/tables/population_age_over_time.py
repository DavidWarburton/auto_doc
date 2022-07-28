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
from itertools import product


class PopulationAgeOverTime(TrackedTable):
    name = 'population_age_over_time'

    def build(self, cursor=None):
        if not cursor:
            cursor = self.get_cursor()
        years = range(1994, 2018)
        age_ranges = [
            [0, 15],
            [16, 17],
            [18, 24],
            [25, 34],
            [35, 44],
            [45, 64],
            [65, 200],
        ]

        aboriginal_indicator_columns = [
            'in_msp',
            'in_mcfd',
            'in_births',
            'in_education',
        ]

        create_or_insert_line = "CREATE TABLE population_age_over_time AS "
        for year in years:
            sql = "SELECT {year} as year".format(year=year)
            for min_age, max_age in age_ranges:
                for p in product([True, False], repeat=4):
                    aboriginal_indicators = dict(zip(aboriginal_indicator_columns, p))

                    sql += (", "
                        "COUNT("
                            "DATE_PART('year', demographics_unduped_dob) - "
                                "{year} BETWEEN {min_age} AND {max_age} AND "
                            "DATE_PART('year', deaths_unduped_date_of_death) - "
                                "{year} >= {year} AND "
                            "aboriginal_status_in_msp = {in_msp} AND "
                            "aboriginal_status_in_mcfd = {in_mcfd} AND "
                            "aboriginal_status_in_births = {in_births} AND "
                            "aboriginal_status_in_education = {in_education}"
                        ") AS population_age_over_time_{min_age}_{max_age}_"
                            "{in_msp}_{in_mcfd}_{in_births}_{in_education}".format(
                            year=year,
                            min_age=min_age,
                            max_age=max_age,
                            **aboriginal_indicators,
                        )
                    )

            sql += " FROM aboriginal_status"
            cursor.execute(
                "{create_or_insert_line} "
                "{sql}".format(
                    create_or_insert_line=create_or_insert_line,
                    sql=sql,
                )
            )

            create_or_insert_line = "INSERT INTO population_age_over_time "
