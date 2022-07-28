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


class MspPrenatal(TrackedTable):
    name = 'msp_prenatal'

    def build(self, cursor=None):
        cur = self.get_cursor()
        cur.execute("""
                    create table msp_prenatal as 
                    select subsidy, servdate, studyid from import_msp_a
                    where feeitem = 14090 or feeitem = 14091
                    group by 1,2,3;
                    
                    insert into msp_prenatal 
                    select subsidy, servdate, studyid from msp_c_redux
                    where feeitem = 14090 or feeitem = 14091
                    group by 1,2,3;
      
        """)
         

    
