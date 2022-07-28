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


class BceaSexYm(TrackedTable):
    name = 'bcea_sex_ym'

    def build(self, cursor=None):
        cur = self.get_cursor()
        cur.execute("""
                    drop table if exists temp_xx;
                    create temp table temp_xx as 
                    select studyida||studyid as studyid, max(trim(gender)) as gender 
                    from bcea_involvement where trim(gender) = 'F' or trim(gender) = 'M'
                    group by 1;
                    
                    create table bcea_sex_ym as 
                    select studyida||studyid as studyid, ym, max(trim(gender)) as ia_gender 
                    from bcea_involvement where trim(gender) = 'F' or trim(gender) = 'M'
                    group by 1,2;
                    
                    drop table if exists temp_xx2;
                    create temp table temp_xx2 as 
                    select studyida||a.studyid as studyid, a.ym, b.studyid as checkxx
                    from bcea_involvement a left outer join bcea_sex_ym b
                    on studyida||a.studyid = b.studyid and a.ym = b.ym ;
                    
                    drop table if exists temp_xx3;
                    create temp table temp_xx3 as 
                    select * from temp_xx2
                    where checkxx is null;
                   
                    insert into bcea_sex_ym 
                    select a.studyid, a.ym, max(coalesce(b.gender,'M')) as ia_gender
                    from temp_xx3 a left outer join temp_xx b
                    on a.studyid = b.studyid
                    group by 1,2;
                   
                    
                    """)
