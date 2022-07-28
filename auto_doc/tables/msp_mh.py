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


class MspMh(TrackedTable):
    name = 'msp_mh'

    def build(self, cursor=None):
        if not cursor:
            cursor = self.get_cursor()
        cursor.execute("""create table msp_mh(studyid text, servdate date, expdamt numeric default 000.00,icd9 text)""")

         
        for yr in range(1985, 2018) :
            print(yr)
            endpart = str.strip(str((yr+1)%100))
            endpart = endpart.zfill(2)
            fn = 'msp_'+str.strip(str(yr))+'_'+endpart
            cursor.execute("""
                drop table if exists temp_zzzz;
                create temp table temp_zzzz as 
                select studyid, servdate, expdamt ,pracnum,
                substring(icd9_1,1,3) as icd9
                from {fn} where (isnumeric(substring(icd9_1,1,3)) and 
                 substring(icd9_1,1,3) between '290' and '319' )
                 or substring(icd9_1,1,3) = '50B';
                
                insert into msp_mh
                select a.studyid, servdate as msp_mh_servdate, expdamt as msp_mh_expdamt ,
                icd9 as  msp_mh_icd9 from temp_zzzz a, msp_pracnum_spec_yr b 
                where pracnum = msp_pracnum_spec_yr_pracnum and 
                extract(year from servdate) = msp_pracnum_spec_yr_yr
                and msp_pracnum_spec_yr_phys ;
                
                drop table if exists temp_zzzz;
                create temp table temp_zzzz as 
                select studyid, servdate, expdamt ,pracnum,
                substring(icd9_2,1,3) as icd9
                from {fn} where (isnumeric(substring(icd9_1,1,3)) and 
                substring(icd9_1,2,3) between '290' and '319' )
                or substring(icd9_2,1,3) = '50B';
                
                insert into msp_mh
                select a.studyid, servdate as msp_mh_servdate, expdamt as msp_mh_expdamt ,
                icd9 as  msp_mh_icd9  
                from temp_zzzz a, msp_pracnum_spec_yr b 
                where pracnum = msp_pracnum_spec_yr_pracnum and 
                extract(year from servdate) = msp_pracnum_spec_yr_yr
                and msp_pracnum_spec_yr_phys ;
                
            """.format(fn = fn))
