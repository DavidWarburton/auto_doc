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


class EducationSchoolStudentYr(TrackedTable):
    name = 'education_school_student_yr'

    def build(self, cursor=None):
        cur = self.get_cursor()

        cur.execute("""
                    
                    create  table education_school_student_yr as
                    select studyid, substring(school_year, 1,4)::int as schl_yr,
                    max(mincode_this_enrol) as mincode_sep, max(student_postal_code_3c) as stud_pcode, max(SCHOOL_POSTAL_CODE) as schl_pcode
                    from education_schlstud 
                    where trim(is_present_sep_30_flag) = 'Present Sept 30'
                    group by 1,2;
                    """)
