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


class GeogInc(TrackedTable):
    name = 'geog_inc'

    def build(self, cursor=None):
        cur = self.get_cursor()
        cur.execute("""
            create table geog_inc as
            select 
                study_id,import_geog_year,
                max(import_geog_qaippe) as  import_geog_qaippe,
                max(import_geog_qnippe) as  import_geog_qnippe, 
                max(import_geog_daippe) as  import_geog_daippe,
                max(import_geog_dnippe) as  import_geog_dnippe
            from import_geog
            group by 1,2
        """)
