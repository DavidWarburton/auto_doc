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


class EducationGradePat(TrackedTable):
    name = 'education_grade_pat'

    def build(self, cursor=None):
        cur = self.get_cursor()
        
        cur.execute("""
                    create table education_grade_pat (study_id text, education_grade_pat_grade_pat text); 
                    """)
        
        cur.execute("""
                    create temporary table temp_9 as
                    select studyid, substr(school_year,1,4)::int as schl_yr,
                    min(case 
                        when  trim(grade_this_enrol) = '01' then '1'
                        when  trim(grade_this_enrol) = '02' then '2'
                        when  trim(grade_this_enrol) = '03' then '3'
                        when  trim(grade_this_enrol) = '04' then '4'
                        when  trim(grade_this_enrol) = '05' then '5'
                        when  trim(grade_this_enrol) = '06' then '6'
                        when  trim(grade_this_enrol) = '07' then '7'
                        when  trim(grade_this_enrol) = '08' then '8'
                        when  trim(grade_this_enrol) = '09' then '9'
                        when  trim(grade_this_enrol) = '10' then 'a'
                        when  trim(grade_this_enrol) = '11' then 'b'
                        when  trim(grade_this_enrol) = '12' then 'c'
                        when  trim(grade_this_enrol) = 'ELEMENTARY'              then 'e'
                        when  trim(grade_this_enrol) = 'GRADUATED ADULT'         then 'g'
                        when  trim(grade_this_enrol) = 'HOME SCHOOLED STUDENT'   then 'd'
                        when  trim(grade_this_enrol) = 'KF'                      then 'f'
                        when  trim(grade_this_enrol) = 'KH'                      then 'h'
                        when  trim(grade_this_enrol) = 'SECONDARY'               then 's'
                        when  trim(grade_this_enrol) = 'UNK'                     then 'u'
                        when  trim(grade_this_enrol) = 'UNSPECIFIED'             then 'v'
                        else 'x'
                    end) as grade_code
                    from education_schlstud
                    where substr(school_year,1,4)::int > 1990
                    group by 1,2 order by 1; 
                    """)

        t_out = cur.execute("""
                    select * from temp_9;
                    """)

        all_involv = cur.fetchall()

        start_pos = 1990 
        blankrecord = '0'*27
        t_id  = '0'*9

        for row in all_involv:
            pos2 = row[1] - (start_pos+1)
            if t_id != row[0]:
                if t_id  != '0'*9 :
                    cur.execute("""
                        insert into education_grade_pat values('{t_id}', '{outrecord}');
                        """.format(t_id= t_id, outrecord=outrecord))
                t_id = row[0]
                outrecord = blankrecord
            t_out = outrecord[:pos2] + row[2] + outrecord[pos2+1:]
            outrecord = t_out
        cur.execute("""
            insert into education_grade_pat values('{t_id}', '{outrecord}');
            """.format(t_id= t_id, outrecord=outrecord))
