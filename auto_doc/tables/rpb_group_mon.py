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


class RpbGroupMon(TrackedTable):
    name = 'rpb_group_mon'

    def build(self, cursor=None):
        cur = self.get_cursor()

        cur.execute("""
                    create table rpb_group_mon (study_id text, grp_pat text); 
                    """)
        cur.execute("""
                    drop  table if exists  rpb_group_mon_wip;
                    create temp table rpb_group_mon_wip as 
                    select study_id,
                            (extract(year from rpb_group_fix_cancel_effectivedate)*12 + extract(month from rpb_group_fix_cancel_effectivedate))::int as startpos,
                            (extract(year from rpb_group_fix_cancel_canceldate)*12 + extract(month from rpb_group_fix_cancel_canceldate)+1)::int as endpos,
                            case when rpb_group_fix_cancel_group_id = 3568820 or rpb_group_fix_cancel_group_id = 2554679 then '4'
                            when rpb_group_fix_cancel_group_id = 92 then '3'
                            when trunc(rpb_group_fix_cancel_group_id/100000) = 56 then '2'
                            else '1' end as grp_type
                    from rpb_group_fix_cancel order by study_id
                    """)
        cur.execute("""
                    select * 
                    from rpb_group_mon_wip order by study_id limit 10
                    """)
        all_involv = cur.fetchall()

        start_pos = 1989 * 12 + 1
        blankrecord = '0'*347
        t_id  = '0'*9

        for row in all_involv:
            pos2 = row[1] - (start_pos+1)
            if pos2 < 1:
                pos2 = 1
            if pos2 > 347:
                pos2 = 347
            pos3 = row[2] - (start_pos+1)
            if pos3 < 1:
                pos3 = 1
            if pos3 > 347:
                pos3 = 347
            print(t_id,row[0], row[1],pos2, row[2], pos3, row[3])
            if t_id != row[0]:
                if t_id  != '0'*9 :
                    cur.execute("""
                        insert into rpb_group_mon values('{t_id}', '{outrecord}');
                        """.format(t_id= t_id, outrecord=outrecord))
                t_id = row[0]
                outrecord = blankrecord
            if pos3 > pos2:
                for pos4 in range(pos2, pos3):
                    t_out = outrecord[:pos4] + row[3] + outrecord[pos4+1:]
                    outrecord = t_out

        cur.execute("""
            insert into rpb_group_mon values('{t_id}', '{outrecord}');
            """.format(t_id= t_id, outrecord=outrecord))
