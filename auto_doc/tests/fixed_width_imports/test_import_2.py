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

from ...auto_doc import FixedWidthImportTable


class ImportTest1(FixedWidthImportTable):
    name = 'import_test_2'
    encoding = 'WIN1252'
    flat_file_paths = {
		"test_flat_file_3": r"R:\working\users\david\auto_doc\auto_doc\tests\fixed_width_imports\flat_files\test_flat_file_3.dat.gz",
        # This file contains the string 'êèêë', and is encoded in cp1252. This makes invalid unicode.
	}
    meta_column_name = "file"
