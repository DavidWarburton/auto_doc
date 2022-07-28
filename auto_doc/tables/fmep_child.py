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


class FmepChild(TrackedTable):
    name = 'fmep_child'

    def build(self, cursor=None):
        cur = self.get_cursor()

        cur.execute("""
                    drop table if exists fmep_child_wip;
                    create table fmep_child_wip(fill text);
                    """)

        with open(r'r:\\working\\data\\idofmep1990-2019.dip_ch_export.A.dat', 'r') as f:
            cur.copy_expert('copy fmep_child_wip from stdin',f)
        f.close()

        cur.execute("""
                    create table fmep_child(case_id_s  text, ch_bdt_yyyymm  text, ch_gender_cd  text, child_seq text,
                    datayr  text, version  text, seqno  text, studyid  text );
                    """)

        cur.execute("select * from fmep_child_wip")
        rows = cur.fetchall()

        for row in rows:
            templine = row[0].translate(str.maketrans("'"," "))
            case_id_s = templine[0:10]
            birth_dt_yyyymm = templine[10:16]
            gender_cd = templine[16:17]
            child_seq = templine[17:29]
            datayr = templine[29:33]
            version = templine[33:35]
            seqno = templine[35:45]
            studyid = templine[45:55]

            cur.execute("""
                insert into fmep_child values('{case_id_s}', '{birth_dt_yyyymm}','{gender_cd}' ,'{child_seq}', '{datayr}', '{version}', '{seqno}', '{studyid}');

            """.format(case_id_s = case_id_s , gender_cd = gender_cd , birth_dt_yyyymm = birth_dt_yyyymm ,child_seq  = child_seq , 
            datayr = datayr , version = version , seqno = seqno , studyid = studyid)) 
