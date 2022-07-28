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
import csv

from io import StringIO
from itertools import zip_longest
from ..auto_doc import TrackedTable


class Study(TrackedTable):
    name = 'study'

    def build(self, cursor=None):
        # Table built to gather universal concepts like study_id
        # into a table so the naming convention is kind to them.
        if not cursor:
            cursor = self.get_cursor()

        cursor.execute(
            "CREATE TABLE study AS "
            "SELECT "
                "study_id, "
                "study_sequence_number, "
                "study_version, "
                "data_year "
            "FROM demographics_unduped"
        )
