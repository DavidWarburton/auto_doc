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


class AllRpbCancel(TrackedTable):
    name = 'all_rpb_cancel'

    def build(self, cursor=None):
        cur = self.get_cursor()
        cur.execute("""
                    create temp table temp_rpb_w_cancel_reason as
                    select studyid, cancelreason, canceldate 
                    from rpblite1986_d where cancelreason is not null
                    group by 1,2,3 ;
                    """)

        for yr in range(1987, 2019) :
            rpb = "rpblite"+ str.strip(str(yr))+"_d" 
            print(rpb)
            cur.execute("""
                        insert into temp_rpb_w_cancel_reason 
                        select studyid, cancelreason, canceldate  
                        from {rpb} where cancelreason is not null
                        group by 1,2,3 ;
                        """.format(rpb = rpb))
        cur.execute("""
                    create table all_rpb_cancel as
                    select studyid, cancelreason, 
                    canceldate
                    from temp_rpb_w_cancel_reason
                    group by 1,2,3 ;
                    """)

