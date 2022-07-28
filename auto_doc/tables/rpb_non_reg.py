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


class RpbNonReg(TrackedTable):
    name = 'rpb_non_reg'

    def build(self, cursor=None):
        cur = self.get_cursor()
        cur.execute("""
                    create table rpb_non_reg (study_id text, rpb_non_reg_pat text); 
                    """)
        # ______________________________________________________________________________________________

            # start date is cancel date due to non payment
        # ______________________________________________________________________________________________

        cur.execute("""
                    create temp table temp_billyy as 
                    select study_id,all_rpb_cancel_cancel_date as start_date,
                        (extract(year from all_rpb_cancel_cancel_date )*12 
                        + extract(month from all_rpb_cancel_cancel_date ))::int as startpos
                    from all_rpb_cancel 
                    where substring(all_rpb_cancel_cancel_reason,1,1) in ('0','4','8','A','P','K','V','Z') 
                    """)
        # ______________________________________________________________________________________________

            # use next effective date as cancel date
        # ______________________________________________________________________________________________

        cur.execute("""
                    create temp table temp_billyy2 as 
                    select a.study_id,startpos , min((extract(year from rpb_group_fix_cancel_effectivedate)*12 
                        + extract(month from rpb_group_fix_cancel_effectivedate))::int) as endpos 
                    from temp_billyy a, rpb_group_fix_cancel b
                    where a.study_id = b.study_id 
                    and rpb_group_fix_cancel_effectivedate > a.start_date
                    group by 1,2
                    """)

 
 
        cur.execute("""
                    select * 
                    from temp_billyy2
                    where startpos is not null and endpos is not null
                    order by study_id
                    """)
        all_involv = cur.fetchall()
                   
                    

        start_pos = 1989 * 12 + 1
        blankrecord = '0'*347
        t_id  = '0'*9

        for row in all_involv:
            pos2 = row[1] - (start_pos+1)
            if pos2 < 1:
                pos2 = 0
            if pos2 > 347:
                pos2 = 347
            pos3 = row[2] - (start_pos+1)
            if pos3 < 1:
                pos3 = 0
            if pos3 > 347:
                pos3 = 347
            # print(t_id,row[0], row[1],pos2, row[2], pos3)
            if t_id != row[0]:
                if t_id  != '0'*9 :
                    cur.execute("""
                        insert into rpb_non_reg values('{t_id}', '{outrecord}');
                        """.format(t_id= t_id, outrecord=outrecord))
                t_id = row[0]
                outrecord = blankrecord
            if pos3 > pos2:
                for pos4 in range(pos2, pos3):
                    t_out = outrecord[:pos4] + '1' + outrecord[pos4+1:]
                    outrecord = t_out

        cur.execute("""
            insert into rpb_non_reg values('{t_id}', '{outrecord}');
            """.format(t_id= t_id, outrecord=outrecord))
