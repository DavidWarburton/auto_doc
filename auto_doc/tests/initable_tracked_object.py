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

import os

from ..auto_doc import TrackedObject

INITABLE_TRACKED_OBJECT_TYPE = 'initable_objects'
class InitableTrackedObject(TrackedObject):
    object_type = INITABLE_TRACKED_OBJECT_TYPE
    subclass_pack_path = str.join('.', [
        __package__,
        INITABLE_TRACKED_OBJECT_TYPE,
    ])
    subclass_file_path = os.path.join(
        os.path.dirname(__file__),
        INITABLE_TRACKED_OBJECT_TYPE,
    )
