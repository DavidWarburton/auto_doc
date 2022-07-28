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


class RpbGroupFixCancel(TrackedTable):
    name = 'rpb_group_fix_cancel'


    def build(self, cursor=None):
        if not cursor:
            cursor = self.get_cursor()
        cursor.execute("""
                    create table rpb_group_fix_cancel as 
                    select distinct studyid, groupid, effectivedate, 
                    case when extract(year from canceldate) > 2018 then 
                    to_date('2018-12-31', 'YYYY-MM-DD')
                    else canceldate end as canceldate 
                    from rpblite2018_d ;
                    drop table if exists rpb_group_all_start;
                    create temp table rpb_group_all_start as 
                    select distinct studyid, groupid, effectivedate, 2018 as yr
                    from rpblite2018_d ;
                    """)


        for yr in range(1986, 2018) :
                rpb = "rpblite"+ str.strip(str(yr))+"_d" 
                cursor.execute("""
                    insert into rpb_group_fix_cancel
                    select distinct studyid, groupid, effectivedate,canceldate 
                    from {rpb} where extract(year from canceldate) <={yr};
                    
                    insert into rpb_group_all_start  
                    select distinct studyid, groupid, effectivedate, {yr}
                    from {rpb};
                            """.format(rpb = rpb, yr = yr))

        cursor.execute("""
                    drop table if exists temp_check_0;
                    create temp table temp_check_0 as 
                    select studyid, groupid, effectivedate, max(yr) as yr 
                    from rpb_group_all_start 
                    group by 1,2,3;
                    
                    drop table if exists temp_check_1;
                    create temp table temp_check_1 as 
                    select distinct * from rpb_group_fix_cancel ;
                    """)
# temp_check_0 is all the starts
# temp_check_1 is all starts with valid stops


        cursor.execute("""
                    drop table if exists temp_check_2;
                    create temp table temp_check_2 as 
                    select distinct a.*, b.effectivedate as check
                    from temp_check_0 a left outer join temp_check_1 b
                    on a.studyid = b.studyid and a.groupid = b.groupid and a.effectivedate = b.effectivedate;
                    """)
# temp_check_2 identifies the starts without a valid stop (check is null)


        # ______________________________________________________________________________________________________

                    # for the registrations with missing end dates, use the start date of the next registration
                    
        # ______________________________________________________________________________________________
            

        cursor.execute("""
                    drop table if exists temp_check_4;
                    create temp table temp_check_4 as 
                    select * from temp_check_2 
                    where temp_check_2.check is null;
                    """)
# temp_check_4 is the starts without a valid stop (check is null)



        cursor.execute("""
                    drop table if exists temp_check_5;
                    create temp table temp_check_5 as 
                    select a.studyid, a.effectivedate, a.groupid, min(b.effectivedate) as canceldate
                    from temp_check_4 a , temp_check_1 b
                    where a.studyid = b.studyid and a.effectivedate < b.effectivedate
                    group by a.studyid, a.effectivedate, a.groupid;
                    """)
# temp_check_5 takes the first start for that person from a record that is after this one



        cursor.execute("""
                    alter table rpb_group_fix_cancel add column fixed integer default 0;
                    insert into rpb_group_fix_cancel 
                    select a.studyid, a.groupid, effectivedate,canceldate, 1 as fixed from temp_check_5 a
                    where extract(year from canceldate) < 2018;
                    """)
                    
                    
#############  
        # ______________________________________________________________________________________________________

                    # how many are still missing?
                    
        # ______________________________________________________________________________________________

        cursor.execute("""
                    drop table if exists temp_check_2;
                    create temp table temp_check_2 as 
                    select distinct a.*, b.effectivedate as check
                    from temp_check_0 a left outer join rpb_group_fix_cancel b
                    on a.studyid = b.studyid and a.groupid = b.groupid and a.effectivedate = b.effectivedate;
                    """)
# temp_check_2 identifies the starts without a valid stop (check is null)



        # ______________________________________________________________________________________________________

                    # for the registrations with missing end dates, use december of the year of the last registration
                    
        # ______________________________________________________________________________________________
            


        cursor.execute("""
                    drop table if exists temp_check_5;
                    create temp table temp_check_5 as 
                    select a.studyid, a.effectivedate, a.groupid, to_date(to_char(yr,'9999')||'-12-31', 'YYYY-MM-DD') as canceldate
                    from temp_check_2 a where a.check is null;
                    """)



        cursor.execute("""
                   
                    insert into rpb_group_fix_cancel 
                    select a.studyid, a.groupid, effectivedate,canceldate, 2 as fixed from temp_check_5 a
                    where extract(year from canceldate) < 2018;

                    """)

