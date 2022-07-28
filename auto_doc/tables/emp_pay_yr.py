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


class EmpPayYr(TrackedTable):
    name = 'emp_pay_yr'

    def build(self, cursor=None):
        cur = self.get_cursor()
        cur.execute("""
            create table emp_pay_yr as select study_id,
            12 - char_length(replace(substring(ses6_pat ,24,12),'9','')) as emp_pay_yr_1991,
            12 - char_length(replace(substring(ses6_pat ,36,12),'9','')) as emp_pay_yr_1992,
            12 - char_length(replace(substring(ses6_pat ,48,12),'9','')) as emp_pay_yr_1993,
            12 - char_length(replace(substring(ses6_pat ,60,12),'9','')) as emp_pay_yr_1994,
            12 - char_length(replace(substring(ses6_pat ,72,12),'9','')) as emp_pay_yr_1995,
            12 - char_length(replace(substring(ses6_pat ,84,12),'9','')) as emp_pay_yr_1996,
            12 - char_length(replace(substring(ses6_pat ,96,12),'9','')) as emp_pay_yr_1997,
            12 - char_length(replace(substring(ses6_pat ,108,12),'9','')) as emp_pay_yr_1998,
            12 - char_length(replace(substring(ses6_pat ,120,12),'9','')) as emp_pay_yr_1999,
            12 - char_length(replace(substring(ses6_pat ,132,12),'9','')) as emp_pay_yr_2000,
            12 - char_length(replace(substring(ses6_pat ,144,12),'9','')) as emp_pay_yr_2001,
            12 - char_length(replace(substring(ses6_pat ,156,12),'9','')) as emp_pay_yr_2002,
            12 - char_length(replace(substring(ses6_pat ,168,12),'9','')) as emp_pay_yr_2003,
            12 - char_length(replace(substring(ses6_pat ,180,12),'9','')) as emp_pay_yr_2004,
            12 - char_length(replace(substring(ses6_pat ,192,12),'9','')) as emp_pay_yr_2005,
            12 - char_length(replace(substring(ses6_pat ,204,12),'9','')) as emp_pay_yr_2006,
            12 - char_length(replace(substring(ses6_pat ,216,12),'9','')) as emp_pay_yr_2007,
            12 - char_length(replace(substring(ses6_pat ,228,12),'9','')) as emp_pay_yr_2008,
            12 - char_length(replace(substring(ses6_pat ,240,12),'9','')) as emp_pay_yr_2009,
            12 - char_length(replace(substring(ses6_pat ,252,12),'9','')) as emp_pay_yr_2010,
            12 - char_length(replace(substring(ses6_pat ,264,12),'9','')) as emp_pay_yr_2011,
            12 - char_length(replace(substring(ses6_pat ,276,12),'9','')) as emp_pay_yr_2012,
            12 - char_length(replace(substring(ses6_pat ,288,12),'9','')) as emp_pay_yr_2013,
            12 - char_length(replace(substring(ses6_pat ,300,12),'9','')) as emp_pay_yr_2014,
            12 - char_length(replace(substring(ses6_pat ,312,12),'9','')) as emp_pay_yr_2015,
            12 - char_length(replace(substring(ses6_pat ,324,12),'9','')) as emp_pay_yr_2016,
            12 - char_length(replace(substring(ses6_pat ,336,12),'9','')) as emp_pay_yr_2017
            from ses6;
        """)

