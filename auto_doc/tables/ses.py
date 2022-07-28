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

class Ses(TrackedTable):
    name = 'ses'
    # Create a dictionary such that index_from_date['1989-02'] = 0, because that's the first date
    # and index_from_date['2017-12'] = 347 because that's the last date.
    first_year = 1989
    last_year = 2017
    index_from_year_month = {
        "{year}-{month}".format(year=year, month=month): i 
        for i, (year, month) in enumerate(
            list(product(
                range(first_year, last_year + 1), # Remember end is not included
                [str(i).rjust(2, '0') for i in range(1,13)],
            ))[1:]
        )
    }

    def build(self, cursor=None):
        cur = self.get_cursor()
        np = 347*'0'
        cur.execute("""
                    create temp table temp_ses_pat as
                    select a.*, coalesce(cnvrt_pgm_pwd(bcea_pgm_pat_pattern) , '{np}') as ia_pat_all_pattern
                    from demographics_unduped a left outer join bcea_pgm_pat  b
                    on a.study_id = b.study_id; 
                    """.format(np = np))

        cur.execute("""
                    create temp table temp_ses_2 as
                    select a.study_id,
                    a.demographics_unduped_gender  ,a.deaths_unduped_date_of_death ,
                    a.demographics_unduped_dob , 
                    merge_ia_group(ia_pat_all_pattern , msp_group_pat ) as merge_pattern
                    from temp_ses_pat a,  msp_group  b
                    where a.study_id = b.study_id  ; 
                    """)
 
        cur.execute("""
                    create table ses as
                    select a.study_id,
                    demographics_unduped_gender , a.deaths_unduped_date_of_death, 
                    demographics_unduped_dob ,
                    merge_subsidies(merge_pattern,msp_subsidy_pat_pat ) as ses_pat
                    from temp_ses_2 a, msp_subsidy_pat  b
                    where a.study_id = b.study_id ; 
                    """)