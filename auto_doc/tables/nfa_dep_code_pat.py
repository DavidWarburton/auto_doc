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


class NfaDepCodePat(TrackedTable):
    name = 'nfa_dep_code_pat'

    def build(self, cursor=None):
        if not cursor:
            cursor = self.get_cursor()
        cursor.execute(
            "CREATE TABLE nfa_dep_code_pat AS "
            "SELECT "
                "study_id, "
                "demographics_unduped_dob, "
                "'' AS nfa_dep_cod_pat_nfa_pattern, "
                "'' AS nfa_dep_cod_pat_dep_code_pattern "
            "FROM demographics_unduped"
        )

        cursor.execute(
            "CREATE TEMPORARY TABLE all_year_months AS "
            "SELECT bcea_cases_studyid_year_month_as_date "
            "FROM bcea_cases_studyid "
            "GROUP BY bcea_cases_studyid_year_month_as_date"
        )

        cursor.execute(
            "CREATE TEMPORARY TABLE empty_year_months AS "
            "SELECT study_id, bcea_cases_studyid_year_month_as_date "
            "FROM nfa_dep_code_pat, all_year_months"
        )

        cursor.execute(
            "CREATE TEMPORARY TABLE nfa_dep_cod_pat_fill_in AS "
            "SELECT "
                "COALESCE(b.bcea_nfa_studyid_no_fixed_address::TEXT, 'E') AS bcea_nfa_studyid_no_fixed_address, "
                "COALESCE(b.bcea_cases_studyid_dependant_relation_code::TEXT, 'E') AS bcea_cases_studyid_dependant_relation_code, "
                "a.bcea_cases_studyid_year_month_as_date, "
                "a.study_id "
            "FROM empty_year_months a LEFT JOIN bcea_cases_studyid b "
            "ON "
                "a.bcea_cases_studyid_year_month_as_date = b.bcea_cases_studyid_year_month_as_date AND "
                "a.study_id = b.study_id "
            "ORDER BY bcea_cases_studyid_year_month_as_date"
        )

        cursor.execute(
            "UPDATE nfa_dep_code_pat a "
            "SET "
                "nfa_dep_cod_pat_nfa_pattern = string_agg_nfa_pattern, "
                "nfa_dep_cod_pat_dep_code_pattern = string_agg_dep_code_pattern "
            "FROM ("
                "SELECT "
                    "STRING_AGG("
                        "COALESCE(bcea_nfa_studyid_no_fixed_address::TEXT, 'N'), "
                        "''"
                    ") AS string_agg_nfa_pattern, "
                    "STRING_AGG("
                        "COALESCE(bcea_cases_studyid_dependant_relation_code::TEXT, 'N'), "
                        "''"
                    ") AS string_agg_dep_code_pattern, "
                    "study_id "
                "FROM nfa_dep_cod_pat_fill_in "
                "GROUP BY study_id "
            ") AS sub_q "
            "WHERE a.study_id = sub_q.study_id "
        )
