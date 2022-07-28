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


class BceaCasesStudyid(TrackedTable):
    name = 'bcea_cases_studyid'

#*****************************************************************************************************
#
#                      
#                      
#
#*****************************************************************************************************


    def build(self, cursor=None):
        cur = self.get_cursor()



        cur.execute("""
            create temp table bcea_involvement_join_nfa as 
            select studyida || studyid AS study_id,fileid,' ' as _program, ' ' as ia_gender, ' ' as _famtype, 
            ' ' as youngest, 
            false as aboriginal_status_in_health ,
            false as aboriginal_status_in_education ,
            false as aborig ,
            ym,
            TO_DATE(ym, 'YYYYMM') AS ym_date,
            birthdt_yyyymm AS fix_bym,
            '0' as nfa,
            '0' as uip_flag,
            '0' as bcea_oop_months_in_bc , 
            case 
                when deprltncd = '*' then 4
                when deprltncd = 'SPO' then 3
                when deprltncd = 'SON' then 2
                when deprltncd = 'DTR' then 1
                else 0 end as deprltncd,
            false as died_next_month,
            false as died_in_next_12_months,
            0 as bcea_cases_studyid_leave,
            0 as bcea_cases_studyid_stop
            from bcea_involvement ;
        """)
        print(1)


        cur.execute("""
            update bcea_involvement_join_nfa a
            SET died_in_next_12_months = (
                CASE WHEN
                    b.deaths_unduped_date_of_death ::date BETWEEN
                        a.ym_date + interval '1 month'
                    AND
                        a.ym_date + interval '13 months' - interval '1 day'
                THEN
                    true
                ELSE
                    false
                END
            ),
            died_next_month  = deaths_unduped_date_of_death::date  BETWEEN
                        a.ym_date + interval '1 month'
                    AND
                        a.ym_date + interval '2 months' - interval '1 day'
            FROM deaths_unduped  b
            WHERE a.study_id = b.study_id;
        """)
        print(2)
        cur.execute("""
            update bcea_involvement_join_nfa a
            SET nfa = '1'
            FROM bcea_nfa  b
            WHERE a.fileid = b._fileid and 
            a.ym = b._ym;
        """)
        print(3)
        cur.execute("""
            update bcea_involvement_join_nfa a
            SET aborig = true
            FROM aboriginal_status  b
            WHERE a.study_id = b.study_id and b.aboriginal_status_in_any ;
        """)
        print(4)
        cur.execute("""
            update bcea_involvement_join_nfa a
            SET ia_gender = b.bcea_sex_ym_ia_gender 
            FROM bcea_sex_ym  b
            WHERE a.study_id = b.study_id and a.ym = b.bcea_sex_ym_year_month ;
        """)
        print(5)
        cur.execute("""
            update bcea_involvement_join_nfa a
            SET _famtype = right(b._famtype,1), 
            _program = b._program, 
            youngest = fix_young(b._spouseage::text, b._youngest,b._famtype)
            FROM bcea_cases  b
            WHERE a.fileid = b._fileid and a.ym = b._ym;
        """)
        print(6)
        cur.execute("""
            update bcea_involvement_join_nfa a
            SET fix_bym = b.bcea_nfa_studyid_dob  
            FROM bcea_bdates  b
            WHERE a.study_id = b.study_id ;
        """)
        print(6)
        cur.execute("""
            update bcea_involvement_join_nfa a
            SET bcea_oop_months_in_bc  = b.bcea_oop_months_in_bc 
            FROM bcea_oop  b
            WHERE a.fileid = trim(b.bcea_ei_pending_file_id)  and 
            a.ym = b.bcea_oop_year_month ;
        """)
        print(7)
        cur.execute("""
            update bcea_involvement_join_nfa a
            SET uip_flag = b.bcea_ei_pending_uip_flag 
            FROM bcea_ei_pending  b
            WHERE a.fileid = trim(b.bcea_ei_pending_file_id)  and 
            a.ym::int = b.bcea_oop_year_month::int;
        """)
        print(8)

        cur.execute("""
            update bcea_involvement_join_nfa a
            SET bcea_cases_studyid_stop = 1
            FROM ia_pat_all  b
            WHERE a.study_id = b.study_id and 
            substr(ia_pat_all_pattern, ymton(ym) - 23868,1) = '0' ;
        """)
        print(9)


        cur.execute("""
            update bcea_involvement_join_nfa a
            SET bcea_cases_studyid_leave = 1
            FROM ia_pat_all  b
            WHERE a.study_id = b.study_id and 
            char_length(replace(substr(ia_pat_all_pattern, ymton(ym) - 23868,12),'0','')) = 0 ;
        """)
        print("9a")



        cur.execute("""
            create table bcea_cases_studyid as
            select study_id,ym as bcea_oop_year_month,
            ym_date as bcea_cases_studyid_year_month_as_date ,
            max(_program) as bcea_cases_studyid_program ,
            max(ia_gender) as bcea_sex_ym_ia_gender ,
            max(_famtype) as bcea_cases_studyid_family_type , 
            max( youngest) as bcea_cases_studyid_youngest , 
            bool_or(aborig) as aboriginal_status_in_any ,
            bool_or(aboriginal_status_in_health) as aboriginal_status_in_health,
            bool_or(aboriginal_status_in_education) as aboriginal_status_in_education,
            max(fix_bym) AS bcea_cases_studyid_fix_bym ,
            max(nfa) as bcea_nfa_studyid_no_fixed_address ,
            max(uip_flag) as bcea_ei_pending_uip_flag ,
            max(bcea_oop_months_in_bc ) as bcea_oop_months_in_bc , 
            max(bcea_cases_studyid_stop) as bcea_cases_studyid_stop,
            max(deprltncd) as bcea_cases_studyid_dependant_relation_code ,
            bool_or(died_next_month) as bcea_cases_studyid_died_next_month ,
            bool_or(died_in_next_12_months) as bcea_cases_studyid_died_in_next_12_months
            from bcea_involvement_join_nfa
            group by 1,2,3;
        """)
