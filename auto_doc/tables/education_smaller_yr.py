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


class EducationSmallerYr(TrackedTable):
    name = 'education_smaller_yr'

    def build(self, cursor=None):
        cur = self.get_cursor()
        cur.execute("""
                    
                    create  table education_smaller_yr as
                    select studyid as study_id, 
                    substring(school_year, 1,4)::int as schl_yr,
                    max(special_need_code_this_coll ) as education_smaller_yr_sn,
                    max( mincode_this_enrol ) as education_smaller_yr_mincode,
                    max(esl_this_coll_flag ) as education_smaller_yr_esl,
                    max(home_lang_group_this_coll ) as  education_smaller_yr_home_lang,
                    max(french_imm_this_coll_flag ) as  education_smaller_yr_fr_imm,
                    max(band_residency_status ) as  education_smaller_yr_band_resident,
                    max(top_lev_org_this_enrol ) as  education_smaller_yr_top_lev_org,
                    max(facility_type_this_enrol ) as  education_smaller_yr_fac_type,
                    max(aboriginal_ever_flag ) as  education_smaller_yr_aborig,
                    max(grade_this_enrol ) as  education_smaller_yr_grade
                    from education_schlstud 
                    where substring(school_year, 1,4)::int > 1990
                    group by 1,2;
                    """)
