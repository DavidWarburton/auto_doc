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


class AboriginalStatus(TrackedTable):
    name = 'aboriginal_status'

    def build(self, cursor=None):
        if not cursor:
            cursor = self.get_cursor()

        cursor.execute("""
            CREATE TABLE aboriginal_status AS
            SELECT * FROM demographics_unduped
        """)

        cursor.execute("ALTER TABLE aboriginal_status ADD COLUMN aboriginal_status_in_any BOOLEAN DEFAULT false")

        cursor.execute("ALTER TABLE aboriginal_status ADD COLUMN aboriginal_status_in_health BOOLEAN DEFAULT false")
        cursor.execute("ALTER TABLE aboriginal_status ADD COLUMN aboriginal_status_in_births BOOLEAN DEFAULT false")
        cursor.execute("""
            CREATE TEMPORARY TABLE in_births AS
            SELECT study_id FROM aboriginal_kids
        """)
        cursor.execute("""
            INSERT INTO in_births
            SELECT study_id FROM aboriginal_moms
        """)
        cursor.execute("""
            INSERT INTO in_births
            SELECT study_id FROM aboriginal_dads
        """)
        cursor.execute("""
            UPDATE aboriginal_status
            SET aboriginal_status_in_births = true, aboriginal_status_in_any = true
            FROM in_births
            WHERE aboriginal_status.study_id = in_births.study_id
        """)

        cursor.execute("ALTER TABLE aboriginal_status ADD COLUMN aboriginal_status_in_msp BOOLEAN DEFAULT false")
        cursor.execute("""
            UPDATE aboriginal_status
            SET aboriginal_status_in_msp = true, aboriginal_status_in_any = true
            FROM status_ind_groupid
            WHERE aboriginal_status.study_id = status_ind_groupid.studyid
        """)

        cursor.execute("ALTER TABLE aboriginal_status ADD COLUMN aboriginal_status_in_education BOOLEAN DEFAULT false")
        cursor.execute("""
            CREATE TEMPORARY TABLE in_education AS
            SELECT studyid
            FROM education_schlstud
            WHERE substring(aboriginal_ever_flag, 1, 1) = 'A'
        """)
        cursor.execute("""
            UPDATE aboriginal_status
            SET aboriginal_status_in_education = true, aboriginal_status_in_any = true
            FROM in_education
            WHERE aboriginal_status.study_id = in_education.studyid
        """)

        cursor.execute("ALTER TABLE aboriginal_status ADD COLUMN aboriginal_status_in_mcfd BOOLEAN DEFAULT false")
        cursor.execute("""
            CREATE TEMPORARY TABLE in_mcfd AS
            SELECT study_id
            FROM import_mcfd_all_client_export
            WHERE import_mcfd_all_client_export_indigenous_origin NOT IN
                ('', 'No', 'Non-Status', 'TBD', 'Unspecified')
        """)
        cursor.execute("""
            UPDATE aboriginal_status
            SET aboriginal_status_in_mcfd = true, aboriginal_status_in_any = true
            FROM in_mcfd
            WHERE aboriginal_status.study_id = in_mcfd.study_id
        """)

        cursor.execute("ALTER TABLE aboriginal_status ADD COLUMN aboriginal_status_in_pssg BOOLEAN DEFAULT false")
        cursor.execute("""
            CREATE TEMPORARY TABLE in_pssg AS
            SELECT studyid
            FROM pssg_offender_tidy
            WHERE aboridnt = '1'
        """)
        cursor.execute("""
            UPDATE aboriginal_status
            SET aboriginal_status_in_pssg = true, aboriginal_status_in_any = true
            FROM in_pssg
            WHERE aboriginal_status.study_id = in_pssg.studyid
        """)

        cursor.execute("ALTER TABLE aboriginal_status ADD COLUMN aboriginal_status_in_deaths BOOLEAN DEFAULT false")
        cursor.execute("""
            CREATE TEMPORARY TABLE in_deaths AS
            SELECT study_id
            FROM deaths_unduped
            WHERE deaths_unduped_native
        """)
        cursor.execute("""
            UPDATE aboriginal_status
            SET aboriginal_status_in_deaths = true, aboriginal_status_in_any = true
            FROM in_deaths
            WHERE aboriginal_status.study_id = in_deaths.study_id
        """)
        cursor.execute("""
            UPDATE aboriginal_status
            SET aboriginal_status_in_health = true
            WHERE aboriginal_status_in_deaths or aboriginal_status_in_msp or aboriginal_status_in_births
        """)
