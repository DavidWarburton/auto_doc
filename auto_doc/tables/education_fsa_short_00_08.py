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


class EducationFsaShort0008(TrackedTable):
    name = 'education_fsa_short_00_08'

    def build(self, cursor=None):
        cur = self.get_cursor()
        
        cur.execute("""
                    create table education_fsa_short_00_08 as
                    select studyid as study_id, 
                    fsa_skill_name as education_fsa_short_00_08_fsa_skill_name,
                    school_year as education_fsa_short_00_08_school_year,
                    fsa_grade as education_fsa_short_00_08_fsa_grade,
                    max(case when isnumeric(irt_scale_score) then irt_scale_score::numeric
                    else -9 end) as education_fsa_short_00_08_irt_scale_score, 
                    max(excused_cnt ) as education_fsa_short_00_08_excused,
                    min(case 
                    when trim(fsa_5_point_scale) = 	'Borderline Between Meets And Exceeds Expectations'          	then 	2
                    when trim(fsa_5_point_scale) = 	'Borderline Between Not Yet Meeting And Meets Expectations'  	then 	4
                    when trim(fsa_5_point_scale) = 	'Exceeds Expectations'                                       	then 	1
                    when trim(fsa_5_point_scale) = 	'FSA Performance Level Unknown'                             	then 	8
                    when trim(fsa_5_point_scale) = 	'Meets Expectations'                                         	then 	3
                    when trim(fsa_5_point_scale) = 	'Not Yet Meeting Expectations'                               	then 	5
                    when trim(fsa_5_point_scale) = 	'Student Did Not Respond'                                    	then 	6
                    when trim(fsa_5_point_scale) = 	'Student Did Not Respond Meaningfully'                       	then 	7
                    when trim(fsa_5_point_scale) = 	'Unspecified'                                                	then 	9
                    else 0 end) as education_fsa_short_00_08_fsa_5_code
                    from
                    education_fsasclsm0 
                    group by 1,2,3,4 ; 
                    """)
