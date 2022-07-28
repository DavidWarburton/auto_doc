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


class GeogPopctrraclass(TrackedTable):
    name = 'geog_popctrraclass'

    def build(self, cursor=None):
        cur = self.get_cursor()
        cur.execute("""
            create table geog_popctrraclass as
            select 
                study_id,import_geog_year,
                max(import_geog_pop_ctr_ra_class) as  import_geog_pop_ctr_ra_class
            from import_geog
            group by 1,2
        """)

