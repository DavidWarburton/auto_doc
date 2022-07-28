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


class Samp20012002(TrackedTable):
    name = 'samp_2001_2002'

    def build(self, cursor=None):
        cur = self.get_cursor()

        cur.execute("""
                drop table if exists temp_samp_2001_2002;
                create temporary table temp_samp_2001_2002 as 
                select distinct a.study_id, demographics_unduped_gender, demographics_unduped_dob, 2001 as yr from
                demographics_unduped a, rpb_contract_fix_cancel  b  
                where a.study_id = b.study_id and to_date('2001-03-15', 'YYYY-MM-DD') between rpb_contract_fix_cancel_start_date and rpb_contract_fix_cancel_cancel_date;
                """)

        cur.execute("""
                insert into temp_samp_2001_2002  
                select distinct a.study_id, demographics_unduped_gender, demographics_unduped_dob, 2002 as yr from
                demographics_unduped a, rpb_contract_fix_cancel  b  
                where a.study_id = b.study_id and to_date('2002-03-15', 'YYYY-MM-DD') between rpb_contract_fix_cancel_start_date and rpb_contract_fix_cancel_cancel_date;
                """)
            
        cur.execute("""
                create temporary table temp_demographics as
                select distinct * from demographics_unduped
                where extract(year from demographics_unduped_dob) between 1978 and 1983;
                
                create temporary table temp_19yolds as
                select distinct a.* from temp_demographics a, rpb_contract_fix_cancel  b
                where a.study_id = b.study_id and demographics_unduped_dob + interval '19 years' between rpb_contract_fix_cancel_start_date and rpb_contract_fix_cancel_cancel_date;
                """)

        cur.execute("""
                    create temporary table temp_2002 as 
                    select * from temp_samp_2001_2002 
                    where yr = 2002;

                    create temporary table temp_19yolds_2002 as
                    select a.*, b.demographics_unduped_dob as test
                    from temp_19yolds a left outer join temp_2002 b
                    on a.study_id = b.study_id;

                    create temporary table temp_2002_fill_ins as
                     select study_id, demographics_unduped_gender, demographics_unduped_dob, 2002 as yr                    from temp_19yolds_2002 where test is null and extract(year from demographics_unduped_dob) between 1979 and 1983;
                    """)

        cur.execute("""
                    create temporary table temp_2001 as 
                    select * from temp_samp_2001_2002 
                    where yr = 2001;

                    create temporary table temp_19yolds_2001 as
                    select a.*, b.demographics_unduped_dob as test
                    from temp_19yolds a left outer join temp_2001 b
                    on a.study_id = b.study_id;
                    create temporary table temp_2001_fill_ins as
                    select study_id, demographics_unduped_gender, demographics_unduped_dob, 2001 as yr                    from temp_19yolds_2001 where test is null and extract(year from demographics_unduped_dob) between 1978 and 1982;
            """)

        cur.execute("""
                    create temporary table temp_fill_ins as
                    select *, 1 as fill_in
                    from temp_2001_fill_ins ;
                    insert into temp_fill_ins
                    select *, 1 as fill_in
                    from temp_2002_fill_ins ;

                    create table samp_2001_2002 as
                    select *, 0 as fill_in
                    from temp_samp_2001_2002;
                    insert into samp_2001_2002 
                    select * from temp_fill_ins;
            """)
