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


class BceaNewStarts(TrackedTable):
    name = 'bcea_new_starts'

    def build(self, cursor=None):
        cur = self.get_cursor()

        cur.execute("""
                    create table bcea_new_starts as
                    select a.study_id, a.bcea_oop_year_month
                    from bcea_adult_cases_studyid a, ia_pat_all b
                    where a.study_id = b.study_id and
                    substring(ia_pat_all_pattern, ymton(a.bcea_oop_year_month) - 23881, 12) = '000000000000'
                    """)
