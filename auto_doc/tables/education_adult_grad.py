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


class EducationAdultGrad(TrackedTable):
    name = 'education_adult_grad'

    def build(self, cursor=None):
        if not cursor:
            cursor = self.get_cursor()
        cursor.execute("""
            select studyid, max(credential_gpa::numeric) as education_adult_grad_gpa,  
            1 as education_adult_grad_agrad, min(credential_issue_month::numeric) as education_adult_grad_agrad_date
            into education_adult_grad 
            from education_studcrd 
            where trim(credential_name) = 'BC ADULT GRADUATION DIPLOMA' 
            group by 1,3
            """)
