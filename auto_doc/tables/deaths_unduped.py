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


class DeathsUnduped(TrackedTable):
    name = 'deaths_unduped'

    def build(self, cursor=None):
        # 20 studyid's have more than one death record
        # 19 have 2 records and one has 1385
        # The studyid with 1385 doesn't link to IA
        # most of the rest have the same death date on both records
        
        cur = self.get_cursor()
        cur.execute("""
            create table deaths_unduped as 
            select 
             
                max(regdt ) as regdt ,
                max(deathdtccyy ) as deathdtccyy ,
                max(deathpc_3) as deathpc_3,
                max(placetype) as placetype,
                max(placeversn) as placeversn,
                max(hosp_) as hosp,
                max(sex) as sex,
                max(marital) as marital,
                bool_or(native) as native,
                bool_or(onreserve) as onreserve,
                max(birthdtccyy) as birthdtccyy,
                max(occupation) as occupation,
                max(workyears) as workyears,
                max(industry) as industry,
                max(prov) as prov,
                max(country) as country,
                max(pc_3) as pc_3,
                max(f_bthprov) as f_bthprov,
                max(f_bthcntry) as f_bthcntry,
                max(m_bthprov) as m_bthprov,
                max(m_bthcntry) as m_bthcntry,
                max(disptype) as disptype,
                max(dispdtccyy) as dispdtccyy,
                max(accdnt_cd) as accdnt_cd,
                max(accdnt_dtccyy) as accdnt_dtccyy,
                bool_or(recnt_surg) as recnt_surg,
                max(surgdtccyy) as surgdtccyy,
                bool_or(cor_bypass) as cor_bypass,
                bool_or(heartvalve) as heartvalve,
                bool_or(transplant) as transplant,
                bool_or(autopsy) as autopsy,
                bool_or(c_autopsy) as c_autopsy,
                bool_or(other_info) as other_info,
                bool_or(lifestyle) as lifestyle,
                bool_or(pregnancy) as pregnancy,
                bool_or(pp_upto42) as pp_upto42,
                bool_or(pp_42to1yr) as pp_42to1yr,
                max(manner_cd) as manner_cd,
                max(activity) as activity,
                max(illnessdtccyy) as illnessdtccyy,
                bool_or(coroner) as coroner,
                bool_or(viewedbody) as viewedbody,
                bool_or(cor_waived) as cor_waived,
                max(certifier) as certifier,
                max(injury_cd) as injury_cd,
                max(icd_versn) as icd_versn,
                max(cause) as cause,
                max(axis_codes) as axis_codes,
                max(datayr) as datayr,
                max(version) as version,
                max(seqno) as seqno,
                studyid
            from deaths_b group by studyid
        """)
