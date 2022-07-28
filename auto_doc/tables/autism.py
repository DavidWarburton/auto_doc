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


class Autism(TrackedTable):
    name = 'autism'

    def build(self, cursor=None):
        cur = self.get_cursor()

        cur.execute("""
            drop table if exists tempxx;
            create temp table tempxx as 
            select studyid, min(to_date(cym,'yyyymm')) as autism_mcfd_date
            from mcfd_cysn_clients 
            where autism = 'Eligible    '
            group by 1;

            drop table if exists tempxx2;
            create temp table tempxx2 as 
            select study_id, min(msp_autism_servdate ) as autism_msp_date 
            from msp_autism
            group by 1;

            drop table if exists tempxx3;
            create temp table tempxx3 as 
            select study_id, min(to_date(TO_CHAR(
            education_smaller_yr_school_year,'9999')||'0930','yyyymmdd'))  as autism_educ_date 
            from education_smaller_yr
            where trim(education_smaller_yr_sn) = 'G'
            group by 1;
            
            drop table if exists tempxx4;
            create temp table tempxx4 as 
            select a.*, autism_mcfd_date
            from demographics_unduped a left outer join tempxx b
            on a.study_id = b.studyid;
            
            drop table if exists tempxx5;
            create temp table tempxx5 as 
            select a.*, autism_msp_date
            from tempxx4 a left outer join tempxx2 b
            on a.study_id = b.study_id;
            
            drop table if exists tempxx6;
            create temp table tempxx6 as 
            select a.*, autism_educ_date
            from tempxx5 a left outer join tempxx3 b
            on a.study_id = b.study_id;
            
            create table autism as
            select * from tempxx6 where
            autism_educ_date is not null or
            autism_msp_date is not null or
            autism_mcfd_date is not null;
        """)
            
