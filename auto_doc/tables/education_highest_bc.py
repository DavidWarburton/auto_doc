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
from .ses6 import Ses6

from datetime import date

ses_year_months = sorted(Ses6.index_from_year_month.keys())

class EducationHighestBc(TrackedTable):
    name = 'education_highest_bc'
    education_data_start_date = date(year=1992, month=1, day=1) # There are a few graduations on
                                                                 # record before this, but not enough
                                                                 # that we can have any confidence that
                                                                 # the graduation for any individual
                                                                 # in the school system before this will
                                                                 # have their graduation recorded
    education_data_end_date = date(year=2017, month=9, day=1)

    def build(self, cursor=None):
        if not cursor:
            cursor = self.get_cursor()
        # First, find people who were at most 16 at when our data started and at least 19 when it ended.
        # We're checking if people were in BC from age 16 and 0 months to age 17 and 11 months
        # And then testing if they graduated before they turned 19.
        cursor.execute("""
            create temp table in_bc as
            select * from aboriginal_status;

            ALTER TABLE in_bc ADD COLUMN education_gr10_schlstud_band_residency_status TEXT;
            ALTER TABLE in_bc ADD COLUMN education_gr10_schlstud_bc_residency_status TEXT;
            UPDATE in_bc a
                SET 
                    education_gr10_schlstud_band_residency_status = b.education_gr10_schlstud_band_residency_status,
                    education_gr10_schlstud_bc_residency_status = b.education_gr10_schlstud_bc_residency_status
            FROM education_gr10_schlstud b 
            WHERE a.study_id = b.study_id;

            ALTER TABLE in_bc ADD COLUMN ses6_pat TEXT;
            UPDATE in_bc a
                SET ses6_pat = b.ses6_pat
            FROM ses6 b
            WHERE a.study_id = b.study_id;

            create temp table temp as 
            select a.*, coalesce(education_highest_grade, ' ') as education_highest_grade
            from in_bc a left outer join education_highest b
            on a.study_id = b.study_id;

            create table education_highest_bc as 
            select a.*, coalesce(education_ever_grad_grad , '0') as education_ever_grad_grad, 
            coalesce(education_ever_grad_gpa, 0) as education_ever_grad_gpa,
            coalesce(education_ever_grad_grad_date, 0) as education_ever_grad_grad_date
            from temp a left outer join education_ever_grad  b
            on a.study_id = b.study_id;

            ALTER TABLE education_highest_bc ADD COLUMN education_adult_grad_gpa NUMERIC;
            ALTER TABLE education_highest_bc ADD COLUMN education_adult_grad_agrad INTEGER;
            ALTER TABLE education_highest_bc ADD COLUMN education_adult_grad_agrad_date NUMERIC;
            ALTER TABLE education_highest_bc ADD COLUMN education_highest_bc_eng_12 BOOLEAN;

            UPDATE education_highest_bc
                SET education_highest_bc_eng_12 = TRUE
            from education_fnlmrk
                WHERE
                combined_curr_code  = 'EN_12_C     ' 
                and studyid = study_id
                and final_letter_grade !='F   ';

            UPDATE education_highest_bc a 
            SET
                education_adult_grad_gpa = coalesce(b.education_adult_grad_gpa),
                education_adult_grad_agrad = coalesce(b.education_adult_grad_agrad),
                education_adult_grad_agrad_date = coalesce(b.education_adult_grad_agrad_date)
            FROM education_adult_grad b 
            WHERE a.study_id = b.study_id;

            ALTER TABLE education_highest_bc
            ADD COLUMN enter_grd_8_year NUMERIC;

            ALTER TABLE education_highest_bc
            ADD COLUMN education_highest_bc_in_possible_grad_pop BOOLEAN DEFAULT FALSE;

            UPDATE education_highest_bc a
                SET 
                    enter_grd_8_year = c.grade_8_year,
                    education_highest_bc_in_possible_grad_pop = TRUE
            FROM (
                SELECT
                    MIN(
                        SUBSTRING(
                            b.school_year,
                            1,
                            4
                        )::int + 8 - TRIM(grade_this_enrol)::int
                    ) AS grade_8_year,
                    b.studyid
                FROM education_schlstud b
                WHERE TRIM(b.grade_this_enrol) IN ('08', '09', '10', '11', '12')
                GROUP BY b.studyid
            ) AS c
            WHERE
                c.studyid = a.study_id;

            ALTER TABLE education_highest_bc
            ADD COLUMN in_bc_16_17 BOOLEAN DEFAULT FALSE;

            UPDATE education_highest_bc
                SET in_bc_16_17 = TRUE
            WHERE
                AGE(DATE '{ses_end_date}-01', demographics_unduped_dob) >= INTERVAL '17 YEARS 11 MONTHS' AND
                AGE(DATE '{ses_start_date}-01', demographics_unduped_dob) <= INTERVAL '16 YEARS';

            UPDATE education_highest_bc
                SET
                    in_bc_16_17 = FALSE
            WHERE
                in_bc_16_17 = True AND
                SUBSTRING(ses6_pat, CAST(12 * (DATE_PART('YEAR', demographics_unduped_dob + INTERVAL '16 YEARS') - 1989) + DATE_PART('MONTH', demographics_unduped_dob) - 1 AS INT), 24) LIKE '%0%';

            UPDATE education_highest_bc
                SET education_highest_bc_in_possible_grad_pop = FALSE
            WHERE in_bc_16_17 = FALSE;

            """.format(
                education_data_end_date=self.education_data_end_date,
                education_data_start_date=self.education_data_start_date,
                ses_start_date=ses_year_months[0],
                ses_end_date=ses_year_months[-1],
            )
        )
