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


class EducationHomeLang(TrackedTable):
    name = 'education_home_lang'

    def build(self, cursor=None):
        cur = self.get_cursor()
        
        cur.execute("""
                    create  table education_home_lang as
                    select studyid as study_id,
                    max(case 
                        when  trim(HOME_LANG_CODE_THIS_COLL) = 'ENGLISH' then '1'
                        when  trim(HOME_LANG_CODE_THIS_COLL) = 'CHINESE' then '2'
                        when  trim(HOME_LANG_CODE_THIS_COLL) = 'MANDARIN' then '2'
                        when  trim(HOME_LANG_CODE_THIS_COLL) = 'CANTONESE' then '2'
                        when  trim(HOME_LANG_CODE_THIS_COLL) = 'PUNJABI' then '2'
                        when  trim(HOME_LANG_CODE_THIS_COLL) = 'UNKNOWN' then '4'
                        when  trim(HOME_LANG_CODE_THIS_COLL) = 'KOREAN' then '5'
                        when  trim(HOME_LANG_CODE_THIS_COLL) = 'TAGALOG (PHILIPINO)' then '6'
                        when  trim(HOME_LANG_CODE_THIS_COLL) = 'PILIPINO' then '6'
                        when  trim(HOME_LANG_CODE_THIS_COLL) = 'SPANISH' then '7'
                        when  trim(HOME_LANG_CODE_THIS_COLL) = 'VIETNAMESE' then '8'
                        when  trim(HOME_LANG_CODE_THIS_COLL) = 'PERSIAN' then '9'
                        when  trim(HOME_LANG_CODE_THIS_COLL) = 'HINDI'              then 'a'
                        when  trim(HOME_LANG_CODE_THIS_COLL) = 'FRENCH'         then 'b'
                        when  trim(HOME_LANG_CODE_THIS_COLL) = 'JAPANESE'   then 'c'
                        when  trim(HOME_LANG_CODE_THIS_COLL) = 'ARABIC'                      then 'd'
                        when  trim(HOME_LANG_CODE_THIS_COLL) = 'PORTUGUESE'                      then 'e'
                        when  trim(HOME_LANG_CODE_THIS_COLL) = 'RUSSIAN'               then 'f'
                        when  trim(HOME_LANG_CODE_THIS_COLL) = 'RUSSIAN'                     then 'f'
                        when  trim(HOME_LANG_CODE_THIS_COLL) = 'URDU'             then 'g'
                        when  trim(HOME_LANG_CODE_THIS_COLL) = 'POLISH'             then 'h'
                        when  trim(HOME_LANG_CODE_THIS_COLL) = 'GERMAN'             then 'i'
                        when  trim(HOME_LANG_CODE_THIS_COLL) = 'OTHER GERMANIC'             then 'i'
                        when  trim(HOME_LANG_CODE_THIS_COLL) = 'DUTCH'             then 'i'
                        when  trim(HOME_LANG_CODE_THIS_COLL) = 'SWEDISH'             then 'i'
                        when  trim(HOME_LANG_CODE_THIS_COLL) = 'DANISH'             then 'i'
                        when  trim(HOME_LANG_CODE_THIS_COLL) = 'NORWEGIAN'             then 'i'
                        when  trim(HOME_LANG_CODE_THIS_COLL) = 'ICELANDIC'             then 'i'
                        else 'x'
                    end) as education_home_lang_code
                    from education_schlstud
                    where substr(school_year,1,4)::int > 1990
                    group by 1 order by 1; 
                    """)