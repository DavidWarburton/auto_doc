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


class FmepCaseStudyid(TrackedTable):
    name = 'fmep_case_studyid'

    def build(self, cursor=None):
        if not cursor:
            cursor = self.get_cursor()
        cursor.execute(
            "CREATE TABLE fmep_case_studyid AS "
            "SELECT * FROM fmep_case"
        )

        cursor.execute("ALTER TABLE fmep_case_studyid ADD COLUMN payor_study_id TEXT")

        cursor.execute(
            "UPDATE fmep_case_studyid a "
            "SET payor_study_id = b.study_id "
            "FROM fmep_payor b "
            "WHERE a.fmep_payor_case_id_s = b.fmep_payor_case_id_s"
        )

        cursor.execute("ALTER TABLE fmep_case_studyid ADD COLUMN recip_study_id TEXT")

        cursor.execute(
            "UPDATE fmep_case_studyid a "
            "SET recip_study_id = b.study_id "
            "FROM fmep_recip b "
            "WHERE a.fmep_payor_case_id_s = b.fmep_payor_case_id_s"
        )
