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


class FamilyCheckPat(TrackedTable):
    name = 'family_check_pat'

    def build(self, cursor=None):
        if not cursor:
            cursor = self.get_cursor()

        cursor.execute(
            "CREATE TABLE family_check_pat AS "
            "SELECT "
                "study_id, "
                "demographics_unduped_dob, "
                "("
                    "parent_pat_pattern::int + "
                    "marriage_pat_pattern::int"
                ")::text AS family_check_pat_pattern"
        )
