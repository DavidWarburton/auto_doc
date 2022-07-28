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


class McfdCarePat(TrackedTable):
    name = 'mcfd_care_pat'


    def build(self, cursor=None):
        cur = self.get_cursor()
        
        cur.execute("""
                    create table mcfd_care_pat (studyid text, care_pat text); 
                    """)
        
        cur.execute("""
                    create temporary table temp_9 as
                    select studyid, ymton(cym) 
                    from mcfd_actv_stage7 
                    group by 1,2 order by 1; 
                    """)

        t_out = cur.execute("""
                    select * from temp_9;
                    """)

        all_involv = cur.fetchall()

        start_pos = 1998 * 12 + 3
        blankrecord = '0'*251
        t_id  = '0'*9

        for row in all_involv:
            pos2 = row[1] - (start_pos+1)
            if t_id != row[0]:
                if t_id  != '0'*9 :
                    cur.execute("""
                        insert into mcfd_care_pat values('{t_id}', '{outrecord}');
                        """.format(t_id= t_id, outrecord=outrecord))
                t_id = row[0]
                outrecord = blankrecord
            t_out = outrecord[:pos2] + '1' + outrecord[pos2+1:]
            outrecord = t_out
        cur.execute("""
            insert into mcfd_care_pat values('{t_id}', '{outrecord}');
            """.format(t_id= t_id, outrecord=outrecord))
