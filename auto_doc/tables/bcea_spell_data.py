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


class BceaSpellData(TrackedTable):
    name = 'bcea_spell_data'

    def build(self, cursor=None):
        cur = self.get_cursor()

        cur.execute("""
                    create table bcea_spell_data as
                    select a.*, censored, leave,dur from 
                    bcea_adult_cases_studyid a, temp_spell_data b
                    where a.study_id = b.studyid and 
                    mon = (substring(bcea_oop_year_month,1,4)::int * 12) + substring(bcea_oop_year_month,5,2)::int -  23869;
                    """)
         
