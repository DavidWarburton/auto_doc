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


class SampG7Fsa(TrackedTable):
    name = 'samp_g7_fsa'

    def build(self, cursor=None):
        # This code makes the sample for the analysis of the impact of the 2002 cuts on
        # performance on the grade 7 fsa's.
        # The sample is all kids in the 2002-2002 sample who were in grade 4 in one of 1999 or 2000.
        # We then add fsa scores and explanatory variables
        
        cur = self.get_cursor()
# extract distinct studyid's for those in grade 4 in 1999 or 2000       
        cur.execute("""
                    create table samp_g7_fsa as
                    select a.* education_grade_pat_grade_pat,
                    ' ' as education_home_lang_code,
                    ' ' as education_sn_pat_sn_pat ,
                    ' ' as education_fi_pat ,
                    ' ' as education_esl_pat ,
                    ' ' as samp_g7_fsa_mincode1999 ,
                    ' ' as samp_g7_fsa_mincode2000  
                    from samp_2001_2002 a, education_grade_pat b 
                    where a.study_id = b.study_id 
                    and position('4' in substring(education_grade_pat_grade_pat,9,2)) > 0; 
                    """)
# attach patterns from SN, FI, home lang, grade, esl       

        cur.execute("""
                    update samp_g7_fsa a set 
                    education_home_lang_code = b.education_home_lang_code 
                    from education_home_lang   b 
                    where a.study_id = b.study_id;
                    
                    update samp_g7_fsa a set 
                    education_sn_pat_sn_pat  = b.education_sn_pat_sn_pat  
                    from education_sn_pat b 
                    where a.study_id = b.study_id;
                    
                    update samp_g7_fsa a set 
                    education_fi_pat  = b.education_fi_pat  
                    from education_fi b 
                    where a.study_id = b.study_id;
                    
                    update samp_g7_fsa a set 
                    education_esl_pat  = b.education_esl_pat  
                    from education_esl b 
                    where a.study_id = b.study_id;
                    
                    update samp_g7_fsa a set 
                    samp_g7_fsa_mincode1999  = b.education_smaller_yr_mincode   
                    from education_smaller_yr b 
                    where a.study_id = b.study_id 
                    and education_smaller_yr_school_year = 1999 ;
                    
                    
                    update samp_g7_fsa a set 
                    samp_g7_fsa_mincode2000  = b.education_smaller_yr_mincode   
                    from education_smaller_yr b 
                    where a.study_id = b.study_id 
                    and education_smaller_yr_school_year = 2000 ;
                    """)

