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


class MspHomeless(TrackedTable):
    name = 'msp_homeless'

    def build(self, cursor=None):
        if not cursor:
            cursor = self.get_cursor()
        cursor.execute("""create table msp_homeless(study_id text, msp_small_servdate date)""")

        
         
        for yr in range(1985, 2018) :
            print(yr)
            endpart = str.strip(str((yr+1)%100))
            endpart = endpart.zfill(2)
            fn = 'msp_'+str.strip(str(yr))+'_'+endpart
            cursor.execute("""
                insert into msp_homeless
                select studyid, servdate 
                from {fn}
                where substring(icd9_1,1,3) = 'V60'
                or substring(icd9_2,1,3) = 'V60'
                or substring(icd9_3,1,3) = 'V60'
                or substring(icd9_4,1,3) = 'V60'
                or substring(icd9_5,1,3) = 'V60'
                ;
            """.format(fn = fn))