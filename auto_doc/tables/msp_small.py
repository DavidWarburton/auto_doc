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


class MspSmall(TrackedTable):
    name = 'msp_small'

    def build(self, cursor=None):
        if not cursor:
            cursor = self.get_cursor()
        cursor.execute("""create table msp_small(study_id text, msp_small_servdate date, msp_small_type numeric)""")

        
         
        for yr in range(1985, 2018) :
            print(yr)
            endpart = str.strip(str((yr+1)%100))
            endpart = endpart.zfill(2)
            fn = 'msp_'+str.strip(str(yr))+'_'+endpart
            cursor.execute("""
                insert into msp_small
                select studyid, servdate, 
                case when substring(icd9_1,1,3) between '290' and '319' or substring(icd9_1,1,3) = '50B' then 1
                    when (substring(icd9_1,1,3) between '670' and '679')
                    or (substring(icd9_1,1,3) between '760' and '799')
                    or (substring(icd9_1,1,1)  = 'V' and substring(icd9_1,2,2)  between '20' and '39')
                    or (substring(icd9_1,3,1)  = 'B' and substring(icd9_1,1,2)  between '30' and '39') then 2
                    else 3 end 
                from {fn};
            """.format(fn = fn))