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


class LeftProvinceByDate(TrackedTable):
    name = 'left_province_by_date'

    def build(self, cursor=None):
        if not cursor:
            cursor = self.get_cursor()
        cursor.execute("""
            CREATE TABLE left_province_by_date AS 
            SELECT 
                COUNT(*), 
                EXTRACT(YEAR FROM rpb_group_fix_cancel_canceldate) AS year, 
                EXTRACT(MONTH FROM rpb_group_fix_cancel_canceldate) AS month 
            FROM rpb_group_fix_cancel a JOIN all_rpb_cancel b ON a.study_id = b.study_id 
            WHERE 
                a.study_id = b.study_id AND 
                a.rpb_group_fix_cancel_canceldate = b.all_rpb_cancel_cancel_date AND 
                b.all_rpb_cancel_cancel_reason = 'E' 
            GROUP BY 2, 3
        """)
