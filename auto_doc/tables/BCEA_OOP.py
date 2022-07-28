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


class BceaOop(TrackedTable):
    name = 'bcea_oop'

    def build(self, cursor=None):
        cur = self.get_cursor()

        cur.execute("""
                    drop table if exists bcea_oop_wip;
                    create temp table bcea_oop_wip(fill text);
                    """)

        with open(r'r:\\working\\data\\idosdpr1996-2009.opp.A.dat', 'r') as f:
            cur.copy_expert('copy bcea_oop_wip from stdin',f)
        f.close()

        cur.execute("""
                    create table bcea_oop(ym text ,fileid text,months_bc text ,datayr text,version text,seqno text);
                    """)

        cur.execute("select * from bcea_oop_wip")
        rows = cur.fetchall()


        for row in rows:
            ym =row[0][0:6]
            fileid =row[0][6:16]
            months_bc =row[0][16:28]
            datayr =row[0][30:34]
            version =row[0][28:30]
            seqno =row[0][34:44]

            cur.execute("""
                insert into bcea_oop values('{ym}' ,  '{fileid}' , '{months_bc}' , '{datayr}' , '{version}' , '{seqno}' );
            """.format(ym  = ym, fileid  = fileid, months_bc  = months_bc, datayr  = datayr, version  = version, seqno = seqno)) 
