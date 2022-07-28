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


class MspSubsidyPat(TrackedTable):
    name = 'msp_subsidy_pat'

    def build(self, cursor=None):
        cur = self.get_cursor()
        
        cur.execute("""
                    create table msp_subsidy_pat(study_id text, msp_subsidy_pat_pat text); 
                    """)
        cur.execute("""
                    select * 
                    from msp_subsidy
                    where msp_subsidy_yr > 1988
                    order by study_id
                    """)
        all_involv = cur.fetchall()

        fill_a = 'a'*12
        fill_b = 'b'*12
        fill_d = 'd'*12
        fill_0 = '0'*12
        fill1_a = 'a'*11
        fill1_b = 'b'*11
        fill1_d = 'd'*11
        fill1_0 = '0'*11
        blankrecord = '0'*347
        t_id  = '0'*9

        for row in all_involv:
            pos2 = int((row[1] - 1989)*12)
            if row[1] > 1989:
                pos2 = pos2 - 1
            # print(row[1], pos2)
            if t_id != row[0]:
                if t_id  != '0'*9 :
                    cur.execute("""
                        insert into msp_subsidy_pat values('{t_id}', '{outrecord}');
                        """.format(t_id= t_id, outrecord=outrecord))
                t_id = row[0]
                outrecord = blankrecord
            if row[2] == True:
                if row[1] == 1989:
                    tfill = fill1_a
                else:
                    tfill = fill_a
            elif row[3] == True :
                if row[1] == 1989:
                    tfill = fill1_d
                else:
                    tfill = fill_d
            elif row[4] == True :
                if row[1] == 1989:
                    tfill = fill1_b
                else:
                    tfill = fill_b
            else:
                if row[1] == 1989:
                    tfill = fill1_0
                else:
                    tfill = fill_0
            
            if row[1] == 1989:
                t_out = outrecord[:pos2] + tfill + outrecord[pos2+11:]
            else:
                t_out = outrecord[:pos2] + tfill + outrecord[pos2+12:]
            outrecord = t_out
                        
        cur.execute("""
            insert into msp_subsidy_pat values('{t_id}', '{outrecord}');
            """.format(t_id= t_id, outrecord=outrecord))
        


