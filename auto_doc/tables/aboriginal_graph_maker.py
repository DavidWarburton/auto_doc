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


class AboriginalGraphMaker(TrackedTable):
    name = 'aboriginal_graph_maker'

    def build(self, cursor=None):
        if not cursor:
            cursor = self.get_cursor()
        cursor.execute(
            "CREATE TABLE aboriginal_graph_maker AS "
            "SELECT * FROM demographics_unduped "
        )

        cursor.execute("ALTER TABLE aboriginal_graph_maker ADD COLUMN education_gr10_yr TEXT")
        cursor.execute(
            "UPDATE aboriginal_graph_maker a "
            "SET "
                "education_gr10_yr = b.education_gr10_yr "
            "FROM education_gr10 b "
            "WHERE "
                "a.study_id = b.study_id"
        )

        cursor.execute("ALTER TABLE aboriginal_graph_maker ADD COLUMN education_gr10_schlstud_AMA_10_c_final NUMERIC")
        cursor.execute("ALTER TABLE aboriginal_graph_maker ADD COLUMN education_gr10_schlstud_AWM_10_C_final NUMERIC")
        cursor.execute("ALTER TABLE aboriginal_graph_maker ADD COLUMN education_gr10_schlstud_EFP_10_C_final NUMERIC")
        cursor.execute("ALTER TABLE aboriginal_graph_maker ADD COLUMN education_gr10_schlstud_EMA_10_C_final NUMERIC")
        cursor.execute("ALTER TABLE aboriginal_graph_maker ADD COLUMN education_gr10_schlstud_EN_10_C_final NUMERIC")
        cursor.execute("ALTER TABLE aboriginal_graph_maker ADD COLUMN education_gr10_schlstud_FMP_10_C_final NUMERIC")
        cursor.execute("ALTER TABLE aboriginal_graph_maker ADD COLUMN education_gr10_schlstud_FRALP_10_C_final NUMERIC")
        cursor.execute("ALTER TABLE aboriginal_graph_maker ADD COLUMN education_gr10_schlstud_MA_10_C_final NUMERIC")
        cursor.execute("ALTER TABLE aboriginal_graph_maker ADD COLUMN education_gr10_schlstud_SC_10_C_final NUMERIC")
        cursor.execute("ALTER TABLE aboriginal_graph_maker ADD COLUMN education_gr10_schlstud_school_year TEXT")
        cursor.execute(
            "UPDATE aboriginal_graph_maker a "
            "SET "
                "education_gr10_schlstud_AMA_10_C_final = b.education_gr10_schlstud_AMA_10_C_final, "
                "education_gr10_schlstud_AWM_10_C_final = b.education_gr10_schlstud_AWM_10_C_final, "
                "education_gr10_schlstud_EFP_10_C_final = b.education_gr10_schlstud_EFP_10_C_final, "
                "education_gr10_schlstud_EMA_10_C_final = b.education_gr10_schlstud_EMA_10_C_final, "
                "education_gr10_schlstud_EN_10_C_final = b.education_gr10_schlstud_EN_10_C_final, "
                "education_gr10_schlstud_FMP_10_C_final = b.education_gr10_schlstud_FMP_10_C_final, "
                "education_gr10_schlstud_FRALP_10_C_final = b.education_gr10_schlstud_FRALP_10_C_final, "
                "education_gr10_schlstud_MA_10_C_final = b.education_gr10_schlstud_MA_10_C_final, "
                "education_gr10_schlstud_SC_10_C_final = b.education_gr10_schlstud_SC_10_C_final, "
                "education_gr10_schlstud_school_year = b.education_gr10_schlstud_school_year "
            "FROM education_gr10_schlstud b "
            "WHERE "
                "a.study_id = b.study_id "
        )

        cursor.execute("ALTER TABLE aboriginal_graph_maker ADD COLUMN aboriginal_status_in_any BOOLEAN DEFAULT false")
        cursor.execute("ALTER TABLE aboriginal_graph_maker ADD COLUMN aboriginal_status_in_births BOOLEAN DEFAULT false")
        cursor.execute("ALTER TABLE aboriginal_graph_maker ADD COLUMN aboriginal_status_in_education BOOLEAN DEFAULT false")
        cursor.execute("ALTER TABLE aboriginal_graph_maker ADD COLUMN aboriginal_status_in_msp BOOLEAN DEFAULT false")
        cursor.execute("ALTER TABLE aboriginal_graph_maker ADD COLUMN aboriginal_status_in_mcfd BOOLEAN DEFAULT false")
        cursor.execute("ALTER TABLE aboriginal_graph_maker ADD COLUMN aboriginal_status_in_pssg BOOLEAN DEFAULT false")
        cursor.execute("ALTER TABLE aboriginal_graph_maker ADD COLUMN aboriginal_status_in_deaths BOOLEAN DEFAULT false")
        cursor.execute("ALTER TABLE aboriginal_graph_maker ADD COLUMN aboriginal_status_in_health BOOLEAN DEFAULT false")

        cursor.execute(
            "UPDATE aboriginal_graph_maker a "
            "SET "
                "aboriginal_status_in_any = b.aboriginal_status_in_any, "
                "aboriginal_status_in_births = b.aboriginal_status_in_births, "
                "aboriginal_status_in_education = b.aboriginal_status_in_education, "
                "aboriginal_status_in_msp = b.aboriginal_status_in_msp, "
                "aboriginal_status_in_pssg = b.aboriginal_status_in_pssg, "
                "aboriginal_status_in_deaths = b.aboriginal_status_in_deaths, "
                "aboriginal_status_in_mcfd = b.aboriginal_status_in_mcfd, "
                "aboriginal_status_in_health = b.aboriginal_status_in_health "
            "FROM aboriginal_status b "
            "WHERE a.study_id = b.study_id"
        )

        cursor.execute("ALTER TABLE aboriginal_graph_maker ADD COLUMN geog_pat_pop_ctr_ra_class_pattern TEXT")
        cursor.execute(
            "UPDATE aboriginal_graph_maker a "
            "SET "
                "geog_pat_pop_ctr_ra_class_pattern = b.geog_pat_pop_ctr_ra_class_pattern "
            "FROM geog_pat b "
            "WHERE a.study_id = b.study_id"
        )

        cursor.execute("ALTER TABLE aboriginal_graph_maker ADD COLUMN nfa_dep_code_pat_nfa_pattern TEXT")
        cursor.execute("ALTER TABLE aboriginal_graph_maker ADD COLUMN nfa_dep_code_pat_dep_code_pattern TEXT")
        cursor.execute(
            "UPDATE aboriginal_graph_maker a "
            "SET "
                "nfa_dep_code_pat_nfa_pattern = b.nfa_dep_code_pat_nfa_pattern, "
                "nfa_dep_code_pat_dep_code_pattern = b.nfa_dep_code_pat_dep_code_pattern "
            "FROM nfa_dep_code_pat b "
            "WHERE a.study_id = b.study_id"
        )

        cursor.execute("ALTER TABLE aboriginal_graph_maker ADD COLUMN ia_pat_all_pattern TEXT")
        cursor.execute(
            "UPDATE aboriginal_graph_maker a "
            "SET ia_pat_all_pattern = b.ia_pat_all_pattern "
            "FROM ia_pat_all b "
            "WHERE a.study_id = b.study_id"
        )

        cursor.execute("ALTER TABLE aboriginal_graph_maker ADD COLUMN ses6_pat TEXT")
        cursor.execute(
            "UPDATE aboriginal_graph_maker a "
            "SET ses6_pat = b.ses6_pat "
            "FROM ses6 b "
            "WHERE a.study_id = b.study_id"
        )

        cursor.execute("ALTER TABLE aboriginal_graph_maker ADD COLUMN education_ever_grad_gpa TEXT")
        cursor.execute("ALTER TABLE aboriginal_graph_maker ADD COLUMN education_ever_grad_grad TEXT")
        cursor.execute("ALTER TABLE aboriginal_graph_maker ADD COLUMN education_ever_grad_grad_date TEXT")
        cursor.execute("ALTER TABLE aboriginal_graph_maker ADD COLUMN education_highest_grade TEXT")
        cursor.execute("ALTER TABLE aboriginal_graph_maker ADD COLUMN education_highest_bc_in_possible_grad_pop BOOLEAN")
        cursor.execute("ALTER TABLE aboriginal_graph_maker ADD COLUMN education_highest_bc_enter_grd_8_year NUMERIC")
        cursor.execute(
            "UPDATE aboriginal_graph_maker a "
            "SET "
                "education_ever_grad_gpa = b.education_ever_grad_gpa, "
                "education_ever_grad_grad = b.education_ever_grad_grad, "
                "education_ever_grad_grad_date = b.education_ever_grad_grad_date, "
                "education_highest_grade = b.education_highest_grade, "
                "education_highest_bc_in_possible_grad_pop = b.education_highest_bc_in_possible_grad_pop, "
                "education_highest_bc_enter_grd_8_year = b.education_highest_bc_enter_grd_8_year "
            "FROM education_highest_bc b "
            "WHERE a.study_id = b.study_id"
        )

        cursor.execute("ALTER TABLE aboriginal_graph_maker ADD COLUMN education_adult_grad_agrad TEXT")
        cursor.execute("ALTER TABLE aboriginal_graph_maker ADD COLUMN education_adult_grad_gpa TEXT")
        cursor.execute("ALTER TABLE aboriginal_graph_maker ADD COLUMN education_adult_grad_agrad_date TEXT")
        cursor.execute("ALTER TABLE aboriginal_graph_maker ADD COLUMN education_smaller_yr_band_resident TEXT")
        cursor.execute("ALTER TABLE aboriginal_graph_maker ADD COLUMN education_smaller_yr_non_res TEXT")
        cursor.execute(
            "UPDATE aboriginal_graph_maker a "
            "SET "
                "education_adult_grad_agrad = b.education_adult_grad_agrad, "
                "education_adult_grad_gpa = b.education_adult_grad_gpa, "
                "education_adult_grad_agrad_date = b.education_adult_grad_agrad_date, "
                "education_smaller_yr_band_resident = b.education_smaller_yr_band_resident, "
                "education_smaller_yr_non_res = b.education_smaller_yr_non_res "
            "FROM education_highest_aborig b "
            "WHERE a.study_id = b.study_id"
        )

        cursor.execute("ALTER TABLE aboriginal_graph_maker ADD COLUMN education_fsa_short_not_excused_participant TEXT")
        cursor.execute("ALTER TABLE aboriginal_graph_maker ADD COLUMN education_fsa_short_excused TEXT")
        cursor.execute("ALTER TABLE aboriginal_graph_maker ADD COLUMN education_fsa_short_school_year TEXT")
        cursor.execute(
            "UPDATE aboriginal_graph_maker a "
            "SET "
                "education_fsa_short_not_excused_participant = b.education_fsa_short_not_excused_participant, "
                "education_fsa_short_excused = b.education_fsa_short_excused, "
                "education_fsa_short_school_year = b.education_fsa_short_school_year "
            "FROM education_fsa_short b "
            "WHERE "
                "a.study_id = b.study_id"
        )

        cursor.execute("ALTER TABLE aboriginal_graph_maker ADD COLUMN aboriginal_graph_maker_grd4_numeracy_fsa_code NUMERIC")
        cursor.execute(
            "UPDATE aboriginal_graph_maker a "
            "SET "
                "aboriginal_graph_maker_grd4_numeracy_fsa_code = b.education_fsa_short_5_code "
            "FROM education_fsa_short b "
            "WHERE "
                "a.study_id = b.study_id AND "
                "TRIM(b.education_fsa_short_skill_name) = 'Numeracy'"
        )

        cursor.execute("ALTER TABLE aboriginal_graph_maker ADD COLUMN aboriginal_graph_maker_grd4_writing_fsa_code NUMERIC")
        cursor.execute(
            "UPDATE aboriginal_graph_maker a "
            "SET "
                "aboriginal_graph_maker_grd4_writing_fsa_code = b.education_fsa_short_5_code "
            "FROM education_fsa_short b "
            "WHERE "
                "a.study_id = b.study_id AND "
                "TRIM(b.education_fsa_short_skill_name) = 'Writing'"
        )

        cursor.execute("ALTER TABLE aboriginal_graph_maker ADD COLUMN aboriginal_graph_maker_grd4_reading_fsa_code NUMERIC")
        cursor.execute(
            "UPDATE aboriginal_graph_maker a "
            "SET "
                "aboriginal_graph_maker_grd4_reading_fsa_code = b.education_fsa_short_5_code "
            "FROM education_fsa_short b "
            "WHERE "
                "a.study_id = b.study_id AND "
                "TRIM(b.education_fsa_short_skill_name) = 'Reading Comprehension'"
        )
