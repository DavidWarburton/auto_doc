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


class GeogUnduped(TrackedTable):
    name = 'geog_unduped'

    def build(self, cursor=None):
        if not cursor:
            cursor = self.get_cursor()
        cursor.execute("""
            CREATE TABLE geog_unduped AS 
            SELECT 
                study_id,
                import_geog_year,
                MAX(
                    CASE
                        WHEN import_geog_daippe = '99' THEN '0'
                        ELSE import_geog_daippe
                    END
                ) as geog_unduped_daippe,
                MAX(import_geog_fsa) as import_geog_fsa,
                MAX(import_geog_postal_code_hash) as import_geog_postal_code_hash,
                MAX(import_geog_imm_ter) as import_geog_imm_ter,
                MAX(study_data_year) as study_data_year,
                MAX(
                    CASE
                        WHEN import_geog_dnippe = '99' THEN '0'
                        ELSE import_geog_dnippe
                    END
                ) as geog_unduped_dnippe,
                MAX(import_geog_sac_code) as import_geog_sac_code,
                MAX(import_geog_csd_uid) as import_geog_csd_uid,
                MAX(
                    CASE
                        WHEN import_geog_fed_uid = '99999' THEN '0'
                        ELSE import_geog_fed_uid
                    END
                ) as geog_unduped_fed_uid,
                MAX(import_geog_sac_type) as import_geog_sac_type,
                MAX(
                    CASE
                        WHEN import_geog_pop_ctr_ra_class = '9' THEN '0'
                        ELSE import_geog_pop_ctr_ra_class
                    END
                ) as geog_unduped_pop_ctr_ra_class,
                MAX(
                    CASE
                        WHEN import_geog_c_size = '9' THEN '0'
                        ELSE import_geog_c_size
                    END
                ) as geog_unduped_c_size,
                MAX(
                    CASE
                        WHEN import_geog_pc_type = '9' THEN '0'
                        ELSE import_geog_pc_type
                    END
                ) as geog_unduped_pc_type,
                MAX(
                    CASE
                        WHEN import_geog_er_uid_2 = '99' THEN '0'
                        WHEN import_geog_er_uid_2 = '9999' THEN '0'
                        ELSE import_geog_er_uid_2
                    END
                ) as geog_unduped_er_uid_2,
                MAX(import_geog_cmap_uid_ct) as import_geog_cmap_uid_ct,
                MAX(import_geog_cma_type) as import_geog_cma_type,
                MAX(import_geog_cmap_uid) as import_geog_cmap_uid,
                MAX(import_geog_cd) as import_geog_cd,
                MAX(import_geog_dmt) as import_geog_dmt,
                MAX(
                    CASE
                        WHEN import_geog_qnippe = '9' THEN '0'
                        ELSE import_geog_qnippe
                    END
                ) as geog_unduped_qnippe,
                MAX(study_version) as study_version,
                MAX(import_geog_fed_16_uid) as import_geog_fed_16_uid,
                MAX(
                    CASE
                        WHEN import_geog_fed_16_uid = '99999' THEN '0'
                        ELSE import_geog_fed_16_uid
                    END
                ) as geog_unduped_fed_16_uid,
                MAX(
                    CASE
                        WHEN import_geog_er_uid = '99' THEN '0'
                        WHEN import_geog_er_uid = '9999' THEN '0'
                        ELSE import_geog_er_uid
                    END
                ) as geog_unduped_er_uid,
                MAX(import_geog_pccf_ver) as import_geog_pccf_ver,
                MAX(
                    CASE
                        WHEN import_geog_qaippe = '9' THEN '0'
                        ELSE import_geog_qaippe
                    END
                ) as geog_unduped_qaippe,
                MAX(import_geog_h_dmt) as import_geog_h_dmt,
                MAX(study_sequence_number) as study_sequence_number,
                MAX(
                    CASE
                        WHEN import_geog_pop_ctr_ra_type = '9' THEN '0'
                        ELSE import_geog_pop_ctr_ra_type
                    END
                ) as geog_unduped_pop_ctr_ra_type,
                MAX(import_geog_c_size_miz) as import_geog_c_size_miz,
                MAX(
                    CASE
                        WHEN import_geog_tracted = '9' THEN '0'
                        ELSE import_geog_tracted
                    END
                ) as geog_unduped_tracted
            FROM import_geog
            GROUP BY 1, 2"""
        )
