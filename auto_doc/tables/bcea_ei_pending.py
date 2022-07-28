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


class BceaEiPending(TrackedTable):
    name = 'bcea_ei_pending'

    def build(self, cursor=None):
        cur = self.get_cursor()

        cur.execute("""
                    drop table if exists bcea_ei_pending_wip;
                    create temp table bcea_ei_pending_wip(fill text);
                    """)

        with open(r'r:\\working\\data\\idosdpr1990-2017.bcea_cases_uipending.A.dat', 'r') as f:
            cur.copy_expert('copy bcea_ei_pending_wip from stdin',f)
        f.close()

        cur.execute("""
                    create table bcea_ei_pending(ym text ,fileid text,uip_flag text ,datayr text,version text,seqno text);
                    """)

        cur.execute("select * from bcea_ei_pending_wip")
        rows = cur.fetchall()

        for row in rows:
            ym =row[0][0:8]
            fileid =row[0][8:18]
            uip_flag =row[0][18:21]
            datayr =row[0][21:25]
            version =row[0][25:27]
            seqno =row[0][27:37]

            cur.execute("""
                insert into bcea_ei_pending values('{ym}' ,  '{fileid}' , '{uip_flag}' , '{datayr}' , '{version}' , '{seqno}' );
            """.format(ym  = ym, fileid  = fileid, uip_flag  = uip_flag, datayr  = datayr, version  = version, seqno = seqno)) 


