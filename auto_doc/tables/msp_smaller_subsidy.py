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


class MspSmallerSubsidy(TrackedTable):
    name = 'msp_smaller_subsidy'

    def build(self, cursor=None):
        cur = self.get_cursor()

        cur.execute("""
            create table msp_smaller_subsidy as
            select studyid, servdate, coalesce(trim(subsidy), 'P') as msp_smaller_subsidy_subsidy 
            from import_msp_c;
            
            insert into msp_smaller_subsidy
            select studyid, servdate, coalesce(trim(subsidy), 'P') as msp_smaller_subsidy_subsidy 
            from import_msp_a;
            
            insert into msp_smaller_subsidy
            select studyid, servdate, coalesce(trim(subsidy), 'P') as msp_smaller_subsidy_subsidy 
            from msp1997_98_fix_sub;
            
            insert into msp_smaller_subsidy
            select studyid, servdate, coalesce(trim(subsidy), 'P') as msp_smaller_subsidy_subsidy 
            from msp1995_96_fix_sub;
            
            insert into msp_smaller_subsidy
            select studyid, servdate, coalesce(trim(subsidy), 'P') as msp_smaller_subsidy_subsidy 
            from  msp1996_97_fix_sub;
                   """)
