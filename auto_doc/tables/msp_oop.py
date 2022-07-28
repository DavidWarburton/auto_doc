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


class MspOop(TrackedTable):
    name = 'msp_oop'

    def build(self, cursor=None):
        cur = self.get_cursor()

# ______________________________________________________________________________________________

			# pull records with close code E 
# ______________________________________________________________________________________________
	



        cur.execute("""
                    create temp table temp as
                    select * 
                    from all_rpb_cancel
                    where substring(trim(all_rpb_cancel_cancel_reason),1,1) = 'E';
                    
                    """)


        # ______________________________________________________________________________________________

                    # find earliest billing after that
        # ______________________________________________________________________________________________
            


        cur.execute("""
                    create temp table temp2 as
                    select a.study_id ,all_rpb_cancel_cancel_date , min(msp_smaller_subsidy_servdate ) as enddate 
                    from temp a, msp_smaller_subsidy b
                    where a.study_id = b.study_id and
                    msp_smaller_subsidy_servdate > all_rpb_cancel_cancel_date + interval '1 month' 
                    group by 1,2;
                    """)
 
        cur.execute("""
                    create temp table temp3 as
                    select a.study_id ,a.all_rpb_cancel_cancel_date, coalesce(enddate, to_date('20200101','yyyymmdd')) as enddate 
                    from temp a left outer join temp2 b
                    on a.study_id = b.study_id and
                    a.all_rpb_cancel_cancel_date = b.all_rpb_cancel_cancel_date ;
                    """)

 
        cur.execute("""
                    create table msp_oop (study_id text, msp_oop_pat text); 
                    """)
                    
        cur.execute("""
                    drop  table if exists  msp_oop_mon_wip;
                    create temp table msp_oop_mon_wip as 
                    select study_id,
                            (extract(year from all_rpb_cancel_cancel_date)*12 + extract(month from all_rpb_cancel_cancel_date))::int as startpos,
                            (extract(year from enddate)*12 + extract(month from enddate)+1)::int as endpos,
                            '1'  as grp_type
                    from temp3 order by study_id
                    """)
        cur.execute("""
                    select * 
                    from msp_oop_mon_wip order by study_id
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
            if t_id != row[0]:
                if t_id  != '0'*9 :
                    cur.execute("""
                        insert into msp_oop values('{t_id}', '{outrecord}');
                        """.format(t_id= t_id, outrecord=outrecord))
                t_id = row[0]
                outrecord = blankrecord
            if pos3 > pos2:
                for pos4 in range(pos2, pos3):
                    t_out = outrecord[:pos4] + row[3] + outrecord[pos4+1:]
                    outrecord = t_out

        cur.execute("""
            insert into msp_oop values('{t_id}', '{outrecord}');
            """.format(t_id= t_id, outrecord=outrecord))
 