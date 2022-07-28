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


class BceaNfaStudyid(TrackedTable):
    name = 'bcea_nfa_studyid'

    def build(self, cursor=None):
        cur = self.get_cursor()
        cur.execute("""
            create table bcea_nfa_studyid as
            select _ym, max(_fileid) as _fileid,max(nfa) as nfa, b.studyida || b.studyid as studyid, birthdt_yyyymm , gender
            from  bcea_nfa  a, bcea_involvement b
            where a._fileid =  b.fileid 
            and a._ym =b.ym and b.studyida !='u' 
            group by 1,4,5,6;
                    """)
