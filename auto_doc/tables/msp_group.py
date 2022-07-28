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


class MspGroup(TrackedTable):
    name = 'msp_group'

    def build(self, cursor=None):
        cur = self.get_cursor()



        cur.execute("""
                    create temp table msp_group_xx (study_id text, msp_group_pat text); 
                    """)
        cur.execute("""
                    create temp table temp_msp_group as 
                    select study_id,
                            (extract(year from rpb_group_fix_cancel_effectivedate)*12 + extract(month from rpb_group_fix_cancel_effectivedate))::int as startpos,
                            (extract(year from rpb_group_fix_cancel_canceldate)*12 + extract(month from rpb_group_fix_cancel_canceldate)+1)::int as endpos,
                            case when rpb_group_fix_cancel_group_id = 3568820 then '4'
                            when rpb_group_fix_cancel_group_id  = 2554679 then '5'
                            when rpb_group_fix_cancel_group_id  = 4915473 or rpb_group_fix_cancel_group_id  = 7747201 then '6'
                            when rpb_group_fix_cancel_group_id  = 2554633 then '7'
                            when rpb_group_fix_cancel_group_id = 92 then '3'
                            when trunc(rpb_group_fix_cancel_group_id/100000) = 56 then '2'
                            else '1' end as grp_type
                    from rpb_group_fix_cancel 
                    where extract(year from rpb_group_fix_cancel_effectivedate)*12 + extract(month from rpb_group_fix_cancel_effectivedate) is not null 
                    and extract(year from rpb_group_fix_cancel_canceldate)*12 + extract(month from rpb_group_fix_cancel_canceldate)+1 is not null
                    order by study_id, 2, 4 desc
                    """)
        cur.execute("""
                    select * 
                    from temp_msp_group order by study_id 
                    """)
        all_involv = cur.fetchall()

        start_pos = 1989 * 12 + 1
        blankrecord = '0'*347
        t_id  = '0'*9

        for row in all_involv:
            pos2 = row[1] - (start_pos+1)
            if pos2 < 0:
                pos2 = 0
            if pos2 > 347:
                pos2 = 347
            pos3 = row[2] - (start_pos+1)
            if pos3 < 1:
                pos3 = 1
            if pos3 > 347:
                pos3 = 347
            if t_id != row[0]:
                if t_id  != '0'*9 :
                    cur.execute("""
                        insert into msp_group_xx values('{t_id}', '{outrecord}');
                        """.format(t_id= t_id, outrecord=outrecord))
                t_id = row[0]
                outrecord = blankrecord
            if pos3 > pos2:
                for pos4 in range(pos2, pos3):
                    t_out = outrecord[:pos4] + row[3] + outrecord[pos4+1:]
                    outrecord = t_out
                        
        cur.execute("""
            insert into msp_group_xx values('{t_id}', '{outrecord}');
            """.format(t_id= t_id, outrecord=outrecord))


        cur.execute("""
                    create table msp_group as
                    select a.*, demographics_unduped_gender , demographics_unduped_dob  
                    from msp_group_xx a, demographics_unduped b
                    where a.study_id = b.study_id; 
                    """)
                    
