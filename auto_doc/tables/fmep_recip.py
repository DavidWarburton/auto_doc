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


class FmepRecip(TrackedTable):
    name = 'fmep_recip'

    def build(self, cursor=None):
        cur = self.get_cursor()

        cur.execute("""
                    drop table if exists fmep_recip_wip;
                    create table fmep_recip_wip(fill text);
                    """)

        with open(r'r:\\working\\data\\idofmep1990-2019.dip_rc_export.A.dat', 'r') as f:
            cur.copy_expert('copy fmep_recip_wip from stdin',f)
        f.close()

        cur.execute("""
                    create table fmep_recip(case_id_s  text, client_id  text, rc_gender_cd  text, rc_bdt_yyyymm  text, rc_postal_3d  text,
                    rc_country_cd  text, rc_cprov_cd  text, gain_effect_dt  text, gain_end_dt  text, datayr  text, version  text, 
                    seqno  text, studyid  text );
                    """)

        cur.execute("select * from fmep_recip_wip")
        rows = cur.fetchall()

        for row in rows:
            templine = row[0].translate(str.maketrans("'"," "))
            case_id_s = templine[0:10]
            client_id = templine[10:30]
            gender_cd = templine[30:31]
            birth_dt_yyyymm = templine[31:37]
            postal_3d = templine[37:40]
            country_cd = templine[40:43]
            prov_cd = templine[43:46]
            gain_effect_dt = templine[46:54]
            gain_end_dt = templine[54:62]
            datayr = templine[62:66]
            version = templine[66:68]
            seqno = templine[68:78]
            studyid = templine[78:88]

            cur.execute("""
                insert into fmep_recip values('{case_id_s}', '{client_id}', '{gender_cd}', '{birth_dt_yyyymm}', '{postal_3d}', '{country_cd}', 
                '{prov_cd}', '{gain_effect_dt}', '{gain_end_dt}','{datayr}', '{version}', '{seqno}', '{studyid}');

            """.format(case_id_s = case_id_s , client_id = client_id , gender_cd = gender_cd , birth_dt_yyyymm = birth_dt_yyyymm , postal_3d = postal_3d , 
            country_cd = country_cd , prov_cd = prov_cd , gain_effect_dt = gain_effect_dt , gain_end_dt = gain_end_dt , 
            datayr = datayr , version = version , seqno = seqno , studyid = studyid)) 
