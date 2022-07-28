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


class BceaInvolvWDeaths(TrackedTable):
    name = 'bcea_involv_w_deaths'

    def build(self, cursor=None):

#*****************************************************************************************************
#
#                      find anomalies in NFA
#                      
#
#*****************************************************************************************************
    
        if not cursor:
            cursor = self.get_cursor()

        cursor.execute("""
            CREATE temp TABLE temp_bcea_involv_w_deaths AS 
                SELECT
                    TO_DATE(ym, 'YYYYMM') AS ym,
                    fileid AS fileid,
                    deprltncd AS deprltncd,
                    birthdt_yyyymm AS birthdt_bcea_involv,
                    gender AS gender_bcea_involv,
                    per_corid AS per_corid,
                    studyida || studyid AS study_id,
                    false AS no_fixed_address,
                    false AS died_within_12_months
                FROM bcea_involvement
        """)

        cursor.execute("""
            UPDATE temp_bcea_involv_w_deaths a
            SET died_within_12_months = (
                CASE WHEN
                    b.deaths_unduped_date_of_death BETWEEN
                        a.ym + interval '1 month'
                    AND
                        a.ym + interval '13 months' - interval '1 day'
                    
                THEN
                    true
                ELSE
                    false
                END
            )
            FROM deaths_unduped  b
                WHERE a.study_id = b.study_id
            AND
                died_within_12_months = false 
        """)

        cursor.execute("""
            UPDATE temp_bcea_involv_w_deaths a
            SET no_fixed_address = true
            FROM bcea_nfa b
            WHERE a.fileid = b._fileid AND a.ym = TO_DATE(b._ym, 'YYYYMM')
        """)
        
        cursor.execute("""
            create table bcea_involv_w_deaths as
            select study_id, ym, bool_or(no_fixed_address) as no_fixed_address,
            bool_or(died_within_12_months) as died_within_12_months
            FROM temp_bcea_involv_w_deaths
            group by 1,2
        """)
