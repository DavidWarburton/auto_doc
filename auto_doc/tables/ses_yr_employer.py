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


class SesYrEmployer(TrackedTable):
    name = 'ses_yr_employer'

    def build(self, cursor=None):
        cur = self.get_cursor()
        cur.execute("""
            create table ses_yr_employer as
            select study_id,
            12 - char_length(replace(substring(ses6_pat,24,12 ),'9','')) ses_yr_employer_1991,
            12 - char_length(replace(substring(ses6_pat,36,12 ),'9','')) ses_yr_employer_1992,
            12 - char_length(replace(substring(ses6_pat,48,12 ),'9','')) ses_yr_employer_1993,
            12 - char_length(replace(substring(ses6_pat,60,12 ),'9','')) ses_yr_employer_1994,
            12 - char_length(replace(substring(ses6_pat,72,12 ),'9','')) ses_yr_employer_1995,
            12 - char_length(replace(substring(ses6_pat,84,12 ),'9','')) ses_yr_employer_1996,
            12 - char_length(replace(substring(ses6_pat,96,12 ),'9','')) ses_yr_employer_1997,
            12 - char_length(replace(substring(ses6_pat,108,12 ),'9','')) ses_yr_employer_1998,
            12 - char_length(replace(substring(ses6_pat,120,12 ),'9','')) ses_yr_employer_1999,
            12 - char_length(replace(substring(ses6_pat,132,12 ),'9','')) ses_yr_employer_2000,
            12 - char_length(replace(substring(ses6_pat,144,12 ),'9','')) ses_yr_employer_2001,
            12 - char_length(replace(substring(ses6_pat,156,12 ),'9','')) ses_yr_employer_2002,
            12 - char_length(replace(substring(ses6_pat,168,12 ),'9','')) ses_yr_employer_2003,
            12 - char_length(replace(substring(ses6_pat,180,12 ),'9','')) ses_yr_employer_2004,
            12 - char_length(replace(substring(ses6_pat,192,12 ),'9','')) ses_yr_employer_2005,
            12 - char_length(replace(substring(ses6_pat,204,12 ),'9','')) ses_yr_employer_2006,
            12 - char_length(replace(substring(ses6_pat,216,12 ),'9','')) ses_yr_employer_2007,
            12 - char_length(replace(substring(ses6_pat,228,12 ),'9','')) ses_yr_employer_2008,
            12 - char_length(replace(substring(ses6_pat,240,12 ),'9','')) ses_yr_employer_2009,
            12 - char_length(replace(substring(ses6_pat,252,12 ),'9','')) ses_yr_employer_2010,
            12 - char_length(replace(substring(ses6_pat,264,12 ),'9','')) ses_yr_employer_2011,
            12 - char_length(replace(substring(ses6_pat,276,12 ),'9','')) ses_yr_employer_2012,
            12 - char_length(replace(substring(ses6_pat,288,12 ),'9','')) ses_yr_employer_2013,
            12 - char_length(replace(substring(ses6_pat,300,12 ),'9','')) ses_yr_employer_2014,
            12 - char_length(replace(substring(ses6_pat,312,12 ),'9','')) ses_yr_employer_2015
            from  ses6;
                    """)
