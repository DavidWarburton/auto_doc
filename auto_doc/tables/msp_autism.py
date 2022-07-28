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


class MspAutism(TrackedTable):
    name = 'msp_autism'

    def build(self, cursor=None):
        if not cursor:
            cursor = self.get_cursor()
        cursor.execute("""create table msp_autism(study_id text, msp_autism_servdate date, msp_autism_pracnum numeric)""")

        
         
        for yr in range(1985, 2018) :
            print(yr)
            endpart = str.strip(str((yr+1)%100))
            endpart = endpart.zfill(2)
            fn = 'msp_'+str.strip(str(yr))+'_'+endpart
            cursor.execute("""
                insert into msp_autism
                select studyid, servdate, pracnum
                from {fn} where substring(icd9_1,1,3) = '299' ;
            """.format(fn = fn))
            
            # column_names = []
            # cursor.execute("select column_name from information_schema.columns where table_name = '{fn}'".format(fn = fn))
            # columns = [col[0] for col in cursor.fetchall()]
            # cursor.execute("select * from {fn}".format(fn = fn))
            # rows = cursor.fetchall()
            # for row in rows:
                # data = {key: value for key, value in zip(columns, row)}
                # autism = False
                # for i in range (1,6):
                    # sub = str.strip(str(i))
                    # var1 = data["icd9_"+sub]
                    # print(var1, var1[:3],var1[:3].isnumeric())
                    # if var1:
                        # if var1[:3].isnumeric():
                            # if int(var1[:3]) == 299 :
                                # autism = True
                   
                # if autism :
                    # cursor.execute("""insert into msp_autism values
                    # ('{studyid}', to_date('{servdate}', 'YYYY-MM-DD'), {pracnum}, '{autism}'::bool);
                    # """.format(studyid  = data["studyid"] ,servdate  = data["servdate"] ,pracnum  = data["pracnum"] ,autism  = autism))
                
 
