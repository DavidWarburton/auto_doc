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


class MspInjuryAssault(TrackedTable):
    name = 'msp_injury_assault'

    def build(self, cursor=None):
        if not cursor:
            cursor = self.get_cursor()
        cursor.execute("""create table msp_injury_assault(studyid text, servdate date, expdamt numeric default 000.00,injury bool, sprain bool, assault bool, xray bool)""")

         
        for yr in range(1985, 2018) :
            print(yr)
            endpart = str.strip(str((yr+1)%100))
            endpart = endpart.zfill(2)
            fn = 'msp_'+str.strip(str(yr))+'_'+endpart
            cursor.execute("""
                drop table if exists temp_zzz;
                create temp table temp_zzz as 
                select studyid, servdate, expdamt ,
                isnumeric(substring(icd9_1,1,3)) and 
                ( substring(icd9_1,1,3) between '800' and '839' or
                substring(icd9_2,1,3) between '800' and '839' or
                substring(icd9_3,1,3) between '800' and '839' or
                substring(icd9_4,1,3) between '800' and '839' or
                substring(icd9_5,1,3) between '800' and '839' or 
                substring(icd9_1,1,3) between '850' and '996' or
                substring(icd9_2,1,3) between '850' and '996' or
                substring(icd9_3,1,3) between '850' and '996' or
                substring(icd9_4,1,3) between '850' and '996' or
                substring(icd9_5,1,3) between '850' and '996')  as injury,
                (substring(icd9_1,1,3) between '840' and '849' or
                substring(icd9_2,1,3) between '840' and '849' or
                substring(icd9_3,1,3) between '840' and '849' or
                substring(icd9_4,1,3) between '840' and '849' or
                substring(icd9_5,1,3) between '840' and '849')  as sprain,
                substring(icd9_1,1,3) ='E96' or
                substring(icd9_2,1,3) ='E96' or
                substring(icd9_3,1,3) ='E96' or
                substring(icd9_4,1,3) ='E96' or
                substring(icd9_5,1,3) ='E96' or
                substring(icd9_1,1,3) ='10A' or
                substring(icd9_2,1,3) ='10A' or
                substring(icd9_3,1,3) ='10A' or
                substring(icd9_4,1,3) ='10A' or
                substring(icd9_5,1,3) ='10A' as assault,
                substring(icd9_1,1,3) ='01X' or
                substring(icd9_2,1,3) ='01X' or
                substring(icd9_3,1,3) ='01X' or
                substring(icd9_4,1,3) ='01X' or
                substring(icd9_5,1,3) ='01X' as xray
                from {fn} a, msp_pracnum_spec_yr b 
                where pracnum = msp_pracnum_spec_yr_pracnum and 
                extract(year from servdate) = msp_pracnum_spec_yr_yr
                and msp_pracnum_spec_yr_phys ;
                
                insert into msp_injury_assault
                select * from temp_zzz where injury or sprain or assault or xray;

            """.format(fn = fn))        
         
                
 
