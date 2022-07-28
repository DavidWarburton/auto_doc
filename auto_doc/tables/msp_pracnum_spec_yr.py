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


class MspPracnumSpecYr(TrackedTable):
    name = 'msp_pracnum_spec_yr'

    def build(self, cursor=None):
        cur = self.get_cursor()
        cur.execute("""
                    create temp table temp1 as 
                    select count(*), pracnum_ as msp_pracnum_spec_yr_pracnum, 
                    spec as msp_pracnum_spec_yr_spec,
                    extract(year from servdate) as msp_pracnum_spec_yr_yr
                    from import_msp_a
                    group by 2,3,4;
                    
                    insert into  temp1 
                    select count(*), pracnum as msp_pracnum_spec_yr_pracnum, 
                    spec as msp_pracnum_spec_yr_spec,
                    extract(year from servdate) as msp_pracnum_spec_yr_yr
                    from msp_c
                    group by 2,3,4;
                    
                    insert into  temp1 
                    select count(*), substring(fill, 10,5)::numeric as msp_pracnum_spec_yr_pracnum, 
                    substring(fill, 15,2)::numeric as msp_pracnum_spec_yr_spec,
                    substring(fill, 2,4)::int as msp_pracnum_spec_yr_yr
                    from msp1995_96_raw
                    group by 2,3,4;
        
                    insert into  temp1 
                    select count(*), substring(fill, 10,5)::numeric as msp_pracnum_spec_yr_pracnum, 
                    substring(fill, 15,2)::numeric as msp_pracnum_spec_yr_spec,
                    substring(fill, 2,4)::int as msp_pracnum_spec_yr_yr
                    from msp1996_97_raw
                    group by 2,3,4;
        
                    insert into  temp1 
                    select count(*), substring(fill, 10,5)::numeric as msp_pracnum_spec_yr_pracnum, 
                    substring(fill, 15,2)::numeric as msp_pracnum_spec_yr_spec,
                    substring(fill, 2,4)::int as msp_pracnum_spec_yr_yr
                    from msp1997_98_raw
                    group by 2,3,4;
        
                    insert into  temp1 
                    select count(*), substring(fill, 10,5)::numeric as msp_pracnum_spec_yr_pracnum, 
                    substring(fill, 15,2)::numeric as msp_pracnum_spec_yr_spec,
                    substring(fill, 2,4)::int as msp_pracnum_spec_yr_yr
                    from msp2010_11_raw
                    group by 2,3,4;
        
        
                    create temp table temp as
                    select msp_pracnum_spec_yr_pracnum,msp_pracnum_spec_yr_yr,
                    max(count) as max_count from temp1
                    group by 1,2;  
                    
                    create table msp_pracnum_spec_yr as 
                    select a.msp_pracnum_spec_yr_pracnum, a.msp_pracnum_spec_yr_yr, min( msp_pracnum_spec_yr_spec) as  msp_pracnum_spec_yr_spec,
                    false as msp_pracnum_spec_yr_phys
                    from temp1 a, temp b
                    where a.msp_pracnum_spec_yr_pracnum = b.msp_pracnum_spec_yr_pracnum
                    and a.msp_pracnum_spec_yr_yr = b.msp_pracnum_spec_yr_yr
                    and count = max_count
                    group by 1,2;

                    update msp_pracnum_spec_yr
                    set 
                    msp_pracnum_spec_yr_phys = is_physician(msp_pracnum_spec_yr_spec )
                    
                    
      
        """)
