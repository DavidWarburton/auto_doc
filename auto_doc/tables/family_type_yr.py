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


class FamilyTypeYr(TrackedTable):
    name = 'family_type_yr'

    def build(self, cursor=None):

        cur = self.get_cursor()
        for yr in range(1991, 2010) :
            tempyr = 'temp_'+str.strip(str(yr))
            fn = "temp_yearly_famtype_"+str.strip(str(yr))+"_id"
            vn1 = "oldest_"+str.strip(str(yr))+"_id"
            vn2 = "second_"+str.strip(str(yr))+"_id"
            ref_date =  str.strip(str(yr))+"-03-31"
            cur.execute("""
                        /*  find all people in R&PB on ref_date*/
                        DROP TABLE IF EXISTS temp_fam_type;
                        create temporary table temp_fam_type as
                        select distinct study_id, rpb_contract_fix_cancel_contract_number as fam, to_date('{ref_date}', 'YYYY-MM-DD') as change_date
                        from rpb_contract_fix_cancel
                        where to_date('{ref_date}', 'YYYY-MM-DD') > rpb_contract_fix_cancel_start_date and to_date('{ref_date}', 'YYYY-MM-DD') <= rpb_contract_fix_cancel_cancel_date;

                        /*  attach birth dates and gender */
                        DROP TABLE IF EXISTS temp_fam_type_1;
                        create temporary table temp_fam_type_1 as
                        select a.study_id, fam, change_date, demographics_unduped_dob,
                        demographics_unduped_gender from temp_fam_type a, demographics_unduped   b
                        where  a.study_id = b.study_id;
                        """.format(ref_date = ref_date,fn = fn)) 



        # ____________________________________________________

                    #  find family type 
                    
        # ______________________________________________________________________________________________



            cur.execute("""
                    DROP TABLE IF EXISTS temp_family_2001;
                    create temporary table temp_family_2001 (id_oldest text, id_second text, fam text, famtype text, change_date date);
                        """)



            cur.execute("select * from temp_fam_type_1 order by fam, demographics_unduped_dob;")
            row = cur.fetchone()
            tid_oldest = row[0]
            tid_second = " "
            tfam = row[1]
            oldest = row[3]
            change_date = row[2]
            ia_age_oldest = change_date.year - row[3].year
            if change_date.month < row[3].month:
                ia_age_oldest = ia_age_oldest - 1
            if ia_age_oldest < 19:
                t_age = "y"
            else:
                t_age = "a"
            count_members = 1
            ttype = "sn"+t_age
            rows = cur.fetchall()
            for row in rows:
                if tfam != row[1]:
                    cur.execute("""
                    insert into temp_family_2001 values('{tid_oldest}', '{tid_second}', '{tfam}', '{ttype}' ,'{change_date}');
                    """.format(tid_oldest = tid_oldest, tid_second = tid_second , change_date = change_date, tfam = tfam, ttype = ttype)) 
                    tid_oldest = row[0]
                    tid_second = " "
                    tfam = row[1]
                    oldest = row[3]
                    change_date = row[2]
                    ia_age_oldest = change_date.year - row[3].year
                    if change_date.month < row[3].month:
                        ia_age_oldest = ia_age_oldest - 1
                    if ia_age_oldest < 19:
                        t_age = "y"
                    else:
                        t_age = "a"
                    count_members = 1
                    ttype = "sn"+t_age
                else:
                    count_members = count_members + 1
                    if count_members == 2:
                        tid_second = row[0]
                        ia_age = row[2].year - row[3].year
                        if row[2].month < row[3].month:
                            ia_age = ia_age - 1
                        if ia_age > 24 or (ia_age >18 and ia_age_oldest - ia_age < 17) or (ia_age >16 and ia_age_oldest - ia_age < 7):
                            ttype = "c"
                        else:
                            ttype = "1p"
                    if count_members == 3 and ttype == "c":
                        ttype = "2p"

                tfam = row[1]

            cur.execute("""
                create temporary table {fn} as 
                select a.study_id, famtype from temp_fam_type a, temp_family_2001 b 
                where a.fam = b.fam;
            """.format(fn = fn))


        cur.execute("""
            create table family_type_yr as
            select *, 1991 as yr from temp_yearly_famtype_1991_id;
        """)
                    

        for yr in range(1992, 2010) :
            fn = "temp_yearly_famtype_"+str.strip(str(yr))+"_id"
            cur.execute("""
                        
                        insert into family_type_yr 
                        select *, {yr} as yr from {fn};
                        """.format(yr = yr,fn = fn))
