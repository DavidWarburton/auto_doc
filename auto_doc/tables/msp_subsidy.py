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
# ______________________________________________________________________________________________

			# From the MSP documentation 
            # Premium assistance rates (subsidy codes) are as follows: 
            # A = 100%, B = 80%, F = 60%, G = 40%, E = 20%, C = no subsidy, 
            # D = 100%  temporary premium assistance, H = 100% paid by social services.
            # from looking online H is full subsidy in 2017 and later, I to N are partial subsidies
            # so group ADH as full subsidy
            # C and blank as no subsidy
            # All others as partial subsidy
# ______________________________________________________________________________________________
	


class MspSubsidy(TrackedTable):
    name = 'msp_subsidy'

    def build(self, cursor=None):
        cur = self.get_cursor()

        cur.execute("""
            create table msp_subsidy as
            select study_id, 
            extract(year from msp_smaller_subsidy_servdate) as msp_subsidy_yr,
            bool_or(msp_smaller_subsidy_subsidy in ('A','D','H')) as msp_subsidy_full, 
            bool_or(msp_smaller_subsidy_subsidy in ('C','P')) as msp_subsidy_none, 
            bool_or(msp_smaller_subsidy_subsidy in ('B','E','F','G','I','J','K','L','M','N' )) as msp_subsidy_partial 
            from msp_smaller_subsidy
            group by 1,2;
                    """)
