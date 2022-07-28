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


class RpbFamPatYr(TrackedTable):
    name = 'rpb_fam_pat_yr'


    def build(self, cursor=None):
        if not cursor:
            cursor = self.get_cursor()
        cursor.execute("""
                    create table rpb_fam_pat_yr (studyid text, fam_pat text); 
                    """)

        t_out = cursor.execute("""
                    select * from family_type_yr order by study_id;
                    """)

        all_involv = cursor.fetchall()

        start_pos = 1991
        blankrecord = '0'*19
        t_id  = '0'*9

        for row in all_involv:
            pos2 = row[2] - (start_pos)
            if t_id != row[0]:
                if t_id  != '0'*9 :
                    cursor.execute("""
                        insert into rpb_fam_pat_yr values('{t_id}', '{outrecord}');
                        """.format(t_id= t_id, outrecord=outrecord))
                t_id = row[0]
                outrecord = blankrecord
            if row[1] == "2p":
                tfam = '1'
            elif row[1] == "sna":
                tfam = '2'
            elif row[1] == "c":
                tfam = '3'
            elif row[1] == "1p":
                tfam = '4'
            elif row[1] == "sny":
                tfam = '5'
            else:
                tfam = '6'
            
            t_out = outrecord[:pos2] + tfam + outrecord[pos2+1:]
            outrecord = t_out
        cursor.execute("""
            insert into rpb_fam_pat_yr values('{t_id}', '{outrecord}');
            """.format(t_id= t_id, outrecord=outrecord))



