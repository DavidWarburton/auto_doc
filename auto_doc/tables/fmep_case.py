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


class FmepCase(TrackedTable):
    name = 'fmep_case'

    def build(self, cursor=None):
        cur = self.get_cursor()

        cur.execute("""
                    drop table if exists fmep_case_wip;
                    create temp table fmep_case_wip(fill text);
                    """)

        with open(r'r:\\working\\data\\idofmep1990-2019.dip_case_export.A.dat', 'r') as f:
            cur.copy_expert('copy fmep_case_wip from stdin',f)
        f.close()

        cur.execute("""
                    create table fmep_case(case_id_s text , prov_court_id text , sup_court_id text , no__of_enr text , 
                    enr_1_start_date text, enr_1_end_date text, enr_2_start_date text, enr_2_end_date text, 
                    enr_3_start_date text, enr_3_end_date text, enr_4_start_date text, enr_4_end_date text, 
                    datayr text , version text , seqno text   );
                    """)

        cur.execute("select * from fmep_case_wip")
        rows = cur.fetchall()

        for row in rows:
            case_id_s = row[0][ 0 : 10 ]
            prov_court_id = row[0][ 10 : 20 ]
            sup_court_id = row[0][ 20 : 30 ]
            no__of_enr = row[0][ 30 : 42 ]
            enr_1_start_date = row[0][ 42 : 50 ]
            enr_1_end_date = row[0][ 50 : 58 ]
            enr_2_start_date = row[0][ 58 : 66 ]
            enr_2_end_date = row[0][ 66 : 74 ]
            enr_3_start_date = row[0][ 74 : 82 ]
            enr_3_end_date = row[0][ 82 : 90 ]
            enr_4_start_date = row[0][ 90 : 98 ]
            enr_4_end_date = row[0][ 98 : 106 ]
            datayr = row[0][ 106 : 110 ]
            version = row[0][ 110 : 112 ]
            seqno = row[0][ 112 : 122 ]
            linefeed = row[0][ 122 : 123 ]

            cur.execute("""
                insert into fmep_case values('{case_id_s}',  '{prov_court_id}',  '{sup_court_id}',  '{no__of_enr}',  
                '{enr_1_start_date}' , '{enr_1_end_date}' , '{enr_2_start_date}' ,
                '{enr_2_end_date}' , '{enr_3_start_date}' , '{enr_3_end_date}' ,
                '{enr_4_start_date}',  '{enr_4_end_date}',  '{datayr}',  '{version}',  '{seqno}'  );

            """.format(case_id_s = case_id_s ,prov_court_id = prov_court_id ,sup_court_id = sup_court_id ,no__of_enr = no__of_enr 
            ,enr_1_start_date = enr_1_start_date ,enr_1_end_date = enr_1_end_date ,enr_2_start_date = enr_2_start_date ,enr_2_end_date = enr_2_end_date ,
            enr_3_start_date = enr_3_start_date ,enr_3_end_date = enr_3_end_date ,enr_4_start_date = enr_4_start_date ,enr_4_end_date = enr_4_end_date ,
            datayr = datayr ,version = version ,seqno = seqno  )) 
