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


class EducationHighestAborig(TrackedTable):
    name = 'education_highest_aborig'

    def build(self, cursor=None):
        # adds on reserve, adult certificate and aboriginal status to education_highest_bc
        if not cursor:
            cursor = self.get_cursor()
        cursor.execute("""
            create table education_highest_aborig as 
            select a.*, coalesce(education_adult_grad_agrad , '0') as education_adult_grad_agrad, 
            coalesce(education_adult_grad_gpa, 0) as education_adult_grad_gpa,
            coalesce(education_adult_grad_agrad_date, 0) as education_adult_grad_agrad_date,
            '                 ' as education_smaller_yr_band_resident,
            '                 ' as education_smaller_yr_non_res,
            False as aboriginal_status_in_education ,
            False as aboriginal_status_in_health
            from  education_highest_bc a left outer join education_adult_grad b
            on a.study_id = b.study_id;

            create temp table BAND
            as select study_id, min(education_smaller_yr_band_resident) as 
            education_smaller_yr_band_resident, min(education_smaller_yr_non_res) as 
            education_smaller_yr_non_res
            from 
            education_smaller_yr group by 1 ;

            UPDATE education_highest_aborig a
                SET education_smaller_yr_band_resident = b.education_smaller_yr_band_resident,
                education_smaller_yr_non_res = b.education_smaller_yr_non_res
                from band b where a.study_id = b.study_id;

            UPDATE education_highest_aborig a
                SET aboriginal_status_in_education  = b.aboriginal_status_in_education ,
                aboriginal_status_in_health  = b.aboriginal_status_in_health 
                from  aboriginal_status b
                where a.study_id = b.study_id;
            """)
