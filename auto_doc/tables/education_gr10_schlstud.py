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


class EducationGr10Schlstud(TrackedTable):
    name = 'education_gr10_schlstud'

    def build(self, cursor=None):
        if not cursor:
            cursor = self.get_cursor()

        cursor.execute(
            "CREATE TABLE education_gr10_schlstud AS "
            "SELECT * FROM aboriginal_status"
        )

        cursor.execute(
            "CREATE TEMPORARY TABLE schlstud_squished AS "
            "SELECT "
                "studyid, "
                "MIN(school_year) FILTER ("
                    "WHERE TRIM(grade_this_enrol) = '10'"
                ") AS school_year, "
                "MIN(band_residency_status) AS education_gr10_schlstud_band_residency_status, "
                "MIN(non_res_this_coll_flag) AS education_gr10_schlstud_bc_residency_status "
            "FROM education_schlstud "
            "GROUP BY studyid"
        )

        cursor.execute("ALTER TABLE education_gr10_schlstud ADD COLUMN school_year TEXT")
        cursor.execute(
            "ALTER TABLE education_gr10_schlstud "
            "ADD COLUMN education_gr10_schlstud_band_residency_status TEXT"
        )
        cursor.execute(
            "ALTER TABLE education_gr10_schlstud "
            "ADD COLUMN education_gr10_schlstud_bc_residency_status TEXT"
        )
        cursor.execute(
            "UPDATE education_gr10_schlstud a "
            "SET "
                "school_year = b.school_year, "
                "education_gr10_schlstud_band_residency_status = b.education_gr10_schlstud_band_residency_status, "
                "education_gr10_schlstud_bc_residency_status = b.education_gr10_schlstud_bc_residency_status "
            "FROM schlstud_squished b "
            "WHERE a.study_id = b.studyid"
        )

        cursor.execute(
            "CREATE TEMPORARY TABLE gr10_unduped AS "
            "SELECT "
                "study_id, "
                "MAX(education_gr10_final) as education_gr10_final, "
                "education_gr10_code "
            "FROM education_gr10 "
            "GROUP BY "
                "study_id, "
                "education_gr10_code"
        )

        cursor.execute(
            "SELECT education_gr10_code "
            "FROM education_gr10 "
            "GROUP BY education_gr10_code"
        )
        courses = [tup[0].strip() for tup in cursor.fetchall() if tup[0].strip()]

        for course in courses:
            cursor.execute(
                "ALTER TABLE education_gr10_schlstud "
                "ADD COLUMN education_gr10_schlstud_{course}_final NUMERIC".format(
                    course=course
                )
            )
            cursor.execute(
                "UPDATE education_gr10_schlstud a "
                "SET "
                    "education_gr10_schlstud_{course}_final = b.education_gr10_final "
                "FROM gr10_unduped b "
                "WHERE a.study_id = b.study_id AND "
                "b.education_gr10_code = '{course}'".format(course=course)
            )
