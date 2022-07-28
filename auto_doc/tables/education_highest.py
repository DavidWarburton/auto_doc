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


class EducationHighest(TrackedTable):
    name = 'education_highest'

    def build(self, cursor=None):
        # This program identifies the highest grade 
        if not cursor:
            cursor = self.get_cursor()
        cursor.execute("""
            create table education_highest as 
            select study_id, max(case
                when education_smaller_yr_grade = '01                     ' then 'd'
                when education_smaller_yr_grade = '02                     ' then 'd'
                when education_smaller_yr_grade = '03                     ' then 'd'
                when education_smaller_yr_grade = '04                     ' then 'd'
                when education_smaller_yr_grade = '05                     ' then 'd'
                when education_smaller_yr_grade = '06                     ' then 'd'
                when education_smaller_yr_grade = '07                     ' then 'd'
                when education_smaller_yr_grade = '08                     ' then 'e'
                when education_smaller_yr_grade = '09                     ' then 'f'
                when education_smaller_yr_grade = '10                     ' then 'g'
                when education_smaller_yr_grade = '11                     ' then 'h'
                when education_smaller_yr_grade = '12                     ' then 'i'
                when education_smaller_yr_grade = 'ELEMENTARY             ' then 'd'
                when education_smaller_yr_grade = 'GRADUATED ADULT        ' then 'c'
                when education_smaller_yr_grade = 'HOME SCHOOLED STUDENT  ' then 'c'
                when education_smaller_yr_grade = 'KF                     ' then 'd'
                when education_smaller_yr_grade = 'KH                     ' then 'd'
                when education_smaller_yr_grade = 'SECONDARY              ' then 'd'
                when education_smaller_yr_grade = 'UNK                    ' then 'a'
                when education_smaller_yr_grade = 'UNSPECIFIED            ' then 'a'
                else ' ' end) as education_highest_grade
                from education_smaller_yr group by 1;
            """)
