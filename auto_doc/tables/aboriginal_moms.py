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


class AboriginalMoms(TrackedTable):
    name = 'aboriginal_moms'

    def build(self, cursor=None):
        # Put code that builds this table here.
        # Use cursor = self.get_cursor() to connect to the database
        # and don't close the cursor when you're done, that's done automatically.
        
        if not cursor:
            cursor = self.get_cursor()
        cursor.execute("""
                    select distinct studyid2 as studyid
                    into aboriginal_moms
                    from births_b 
                    where m_astatus ='Y'
                    """)
