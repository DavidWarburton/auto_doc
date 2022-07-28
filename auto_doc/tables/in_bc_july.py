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
import datetime	
from dateutil.relativedelta import *


class InBcJuly(TrackedTable):
    name = 'in_bc_july'

    def build(self, cursor=None):
        cur = self.get_cursor()
        startmonth = datetime.datetime.strptime("1989-01-01", "%Y-%m-%d").date()

        cur.execute("""

                    create temp table all_july as 
                    select study_id, demographics_unduped_dob,substring(ses6_pat, {mon},1) as ses6_ses,
                    demographics_unduped_gender ,{ym} as ym
                    from ses6
                    where substring(ses6_pat, {mon},1) != '0'
                    and to_date('{ym}','YYYYmm') between demographics_unduped_dob and deaths_unduped_date_of_death;
                    """.format(mon = 6, ym = '198907'))
                       


        for mon in range(18,358,12):
            curmonth = startmonth + relativedelta(months = +mon)
            ym = curmonth.strftime("%Y%m")
            print(mon, ym)
            cur.execute("""
                    insert into all_july 
                    select study_id, demographics_unduped_dob,substring(ses6_pat, {mon},1) as ses6_ses,
                    demographics_unduped_gender ,{ym} as ym
                    from ses6
                    where substring(ses6_pat, {mon},1) != '0'
                    and to_date('{ym}','YYYYmm') between demographics_unduped_dob and deaths_unduped_date_of_death;
                     """.format(mon = mon, ym = ym))
                          
        cur.execute("""
                    create table in_bc_july as 
                    select a.*,aboriginal_status_in_health ,
                    aboriginal_status_in_education 
                    from all_july a, aboriginal_status b 
                    where a.study_id = b.study_id;
                    """)
