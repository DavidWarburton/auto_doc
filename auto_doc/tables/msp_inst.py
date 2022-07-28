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


class MspInst(TrackedTable):
    name = 'msp_inst'

    def build(self, cursor=None):
        cur = self.get_cursor()

        cur.execute("""
                    create table msp_inst as
                    select studyid, servdate
                    from import_msp_a
                    where servcode = 7 or servcode = 27
                    group by 1,2;
                    
                    insert into msp_inst 
                    select studyid, servdate
                    from msp_c
                    where servcode = 7 or servcode = 27
                    group by 1,2;
                    """)
