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


class EducationSnPat(TrackedTable):
    name = 'education_sn_pat'

    def build(self, cursor=None):
        cur = self.get_cursor()
        
        cur.execute("""
                    create table education_sn_pat (study_id text, education_sn_pat_sn_pat text); 
                    """)
        
        cur.execute("""
                    create temporary table temp_sn_pat as
                    select study_id, education_smaller_yr_school_year ,
                    min(case 
                       when  trim(education_smaller_yr_sn) = 'NON SPECIAL NEED' then 'x'
                       else trim(education_smaller_yr_sn)
                    end) as sn_code
                    from education_smaller_yr 
                    where education_smaller_yr_school_year > 1990
                    group by 1,2 order by 1; 
                    """)

        t_out = cur.execute("""
                    select * from temp_sn_pat;
                    """)

        all_involv = cur.fetchall()

        start_pos = 1990 
        blankrecord = '0'*27
        t_id  = '0'*9

        for row in all_involv:
            if len(row[2]) !=1:
                print(row[0],row[1],row[2])
            pos2 = row[1] - (start_pos+1)
            if t_id != row[0]:
                if t_id  != '0'*9 :
                    cur.execute("""
                        insert into education_sn_pat values('{t_id}', '{outrecord}');
                        """.format(t_id= t_id, outrecord=outrecord))
                t_id = row[0]
                outrecord = blankrecord
            t_out = outrecord[:pos2] + row[2] + outrecord[pos2+1:]
            outrecord = t_out
        cur.execute("""
            insert into education_sn_pat values('{t_id}', '{outrecord}');
            """.format(t_id= t_id, outrecord=outrecord))
