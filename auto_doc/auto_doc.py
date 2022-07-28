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

import psycopg2
import json
import glob
import gzip
import pandas
import warnings
import inspect
import hashlib
import shutil
import os

from struct import pack
from io import BytesIO, StringIO
from matplotlib import pyplot
from importlib import import_module
from datetime import datetime
from importlib.util import spec_from_file_location, module_from_spec
from pathlib import Path
from copy import copy, deepcopy
from collections import OrderedDict

from . import settings


RESERVED_TABLE_NAMES = ['code_tracker', 'update_tracker']

def out_of_place_update(dict_1, dict_2):
    result = copy(dict_1)
    result.update(dict_2)
    return result


class TrackedProperties(type):

    def __init__(klass, *args, **kwargs):
        """
        Subclasses of TrackedObject must define object_type, and
        may define subclass_pack_path and/or documentation_path.

        This metaclass defines useful defaults for subclass_pack_path and
        documentation_path. This must be done on the
        metaclass because these values are used in a @classmethod on
        TrackedObject.

        object_type:         String containing the plural name of the object
                             this TrackedObject subclass is set up to track.
                             Eg. 'tables', 'columns'

        subclass_pack_path:  Should be a valid package path (connected with
                             dots) at which to store python files which define
                             subclasses of a subclass with build instructions
                             for a single specific object
                             Cannot be outside __package__

        subclass_file_path:  Same location as in subclass_pack_path, but in as
                             as a file path.

        documentation_path:  Should be a valid file path (using os.path) at 
                             which to store json documentation files. Should
                             be accessed through the property, which provides
                             a useful default based on the subclass_pack_path
                             Cannot be outside __package__
        """

        if getattr(klass, 'object_type', None) and klass.object_type != 'objects':

            if (
                not hasattr(klass, 'subclass_pack_path') or
                not klass.subclass_pack_path
            ):
                klass.subclass_pack_path = str.join('.', [
                    __package__,
                    klass.object_type,
                ])

            if (
                not hasattr(klass, 'subclass_file_path') or
                not klass.subclass_file_path
            ):
                subclass_pack_init = import_module(klass.subclass_pack_path)
                klass.subclass_file_path = os.path.dirname(subclass_pack_init.__file__)

            assert klass.subclass_file_path.endswith(
                os.path.join(*klass.subclass_pack_path.split('.'))
            ), "Subclass file path and package path do not match for {klass}".format(klass=klass.__name__)

            if (
                not hasattr(klass, 'documentation_path') or
                not klass.documentation_path
            ):
                klass.documentation_path = (
                    os.path.join(
                        klass.subclass_file_path,
                        'documentation',
                    )
                )


class TrackedObject(metaclass=TrackedProperties):
    """
    This class defines an object which is built with python code and stored in the database.

    Subclasses of this class should be tailored to a specific type of object (table, column, ect.)

    Subclasses of those subclasses should define how to build a single specific object.
    """

    host = settings.host
    db = settings.db
    schema = settings.schema
    common_user = settings.common_user
    auto_index_columns = settings.auto_index_columns
    coded_column_cap = settings.coded_column_cap
    quick_analyze_limit = settings.quick_analyze_limit

    empty_documentation = {
        'description': '',
        'dependencies': [],
        'descendents': [],
        'column_data': {},
        'column_stats': {},
        'table_data': {},
        'table_bytes': 0,
        'code_hash': '',
    }

    is_updated = False
    prefix = None
    code_template = ''
    object_type = 'objects'
    buildable = True
    application_name = 'auto_doc'
    code_tracking_table_name = 'code_tracker'
    save_point_prefix = 'auto_doc_sp'

    def __init__(self, documentation, name=None, test_mode=False):

        if not getattr(self, 'name', None):
            if name:
                self.name = name
            else:
                raise NotImplementedError(
                    "Subclasses of subclasses of TrackedObject must define "
                    "a 'name' attribute."
                )

        if self.name in RESERVED_TABLE_NAMES:
            raise NotImplementedError(
                "The name {name} is reserved for internal use by auto_doc".format(
                    name=self.name,
                )
            )

        if self.prefix and not self.name.startswith('{prefix}_'.format(prefix=self.prefix)):
            raise NotImplementedError(
                "{object_type} names must start with '{prefix}_'".format(
                    object_type=self.object_type,
                    prefix=self.prefix,
                )
            )

        self.documentation = out_of_place_update(
            self.empty_documentation,
            documentation,
        )

        self._db_snapshot = {}
        self.exists = False
        self.changed = False

        self._connection = None
        self._cursor = None

    @classmethod
    def build_code_template(klass, table_name, code_template=None, extra_formating={}):
        if not code_template:
            code_template = klass.code_template

        pkg_index = klass.subclass_pack_path.find(__package__)
        relative_path = '.' * (
            1 + klass.subclass_pack_path[pkg_index + len(__package__):].count('.')
        )

        return code_template.format(
            table_class_name=''.join(
                [word.capitalize() for word in table_name.lower().split('_')]
            ), # Convert table name from PostgreSQL format (table_name) to python class format (TableName)
            table_name=table_name,
            package=relative_path + __package__,
            klass=klass.__name__,
            **extra_formating,
        )

    @classmethod
    def get_tracked_object_types(klass, tracked_types=[]):
        """
        Returns a list of all direct subclasses of TrackedObject.
        Also checks to makes sure that no two of these subclasses
        define the same object_type.
        """

        tracked_object = klass
        while not tracked_object.object_type == 'objects':
            for parent in tracked_object.__bases__:
                if hasattr(parent, 'object_type'):
                    tracked_object = parent
                    break
            else:
                break

        tracked_types = {}
        tidy_tracked_types = {}
        for tracked_type in tracked_object.__subclasses__():
            tracked_key = "{object_type}_{prefix}".format(
                object_type=tracked_type.object_type,
                prefix=tracked_type.prefix,
            )
            if tracked_type.object_type == 'objects':
                raise KeyError(
                    "Subclass of TrackedObject used reserved object_type 'objects'"
                )
            elif tracked_key not in tracked_types:
                tracked_types[tracked_key] = tracked_type
                tidy_tracked_types[tracked_type.object_type] = tracked_type
            else:
                raise KeyError(
                    "Two subclasses of TrackedObject define the same object_type or prefix"
                    "object_type: {object_type_1}, {object_type_2} "
                    "prefix: {prefix_1}, {prefix_2} "
                    "class_typs: {tracked_type_1}, {tracked_type_2}".format(
                        object_type_1=tracked_type.object_type,
                        object_type_2=tracked_types[tracked_key].object_type,
                        prefix_1=tracked_type.prefix,
                        prefix_2=tracked_types[tracked_key].prefix,
                        tracked_type_1=tracked_types[tracked_type.object_type],
                        tracked_type_2=tracked_type,
                    )
                )
        return tidy_tracked_types

    @classmethod
    def load_documentation(klass):
        """
        Load all documentation files from documentation_path
        """
        documentation = {}

        for filename in os.listdir(klass.documentation_path):
            abs_filename = os.path.join(klass.documentation_path, filename)
            with open(abs_filename) as doc_file:
                try:
                    doc_data = json.load(doc_file)
                except json.decoder.JSONDecodeError as e:
                    raise json.decoder.JSONDecodeError(
                        "{filename} has json errors".format(
                            filename=abs_filename,
                        ),
                        e.doc,
                        e.pos,
                    ) from e

                if len(doc_data.keys()) != 1:
                    raise KeyError(
                        "Documentation file at {abs_filename} has two top "
                        "level keys. There should be only one which is the "
                        "object's name.".format(abs_filename==abs_filename)
                    )

                documentation.update(doc_data)

        return documentation

    @classmethod
    def get_all(klass):
        """
        Load all subclasses of a particular subclass of TrackedObject
        that share an object_type with klass.
        """

        # load subclass parent module to allow relative imports within subclasses
        table_modules = import_module(klass.subclass_pack_path)

        # get all .py files in the subclass folder with glob
        module_paths = set(
            glob.glob(
                os.path.join(
                    os.path.dirname(table_modules.__file__),
                    "*.py",
                )
            )
        )

        # load all subclass modules
        for module_path in module_paths:
            module_name = '.'.join([
                klass.subclass_pack_path,
                os.path.splitext(os.path.basename(module_path))[0],
            ])
            import_module(module_name)

        tracked_types = klass.get_tracked_object_types()

        tracked_subclasses = tracked_types[klass.object_type].__subclasses__()
        documentation = klass.load_documentation()

        tracked_objects = {}
        for tracked_object in tracked_subclasses:
            tracked_objects[tracked_object.name] = tracked_object(documentation.get(tracked_object.name, {}))

        return tracked_objects

    @classmethod
    def most_common_vals_to_set(klass, most_common_val_str):
        if most_common_val_str is None:
            working_most_common_val_str = ''
        else:
            working_most_common_val_str = most_common_val_str.lstrip('{').rstrip('}')

        if not working_most_common_val_str:
            return set()
        else:
            return set(working_most_common_val_str.split(','))

    @classmethod
    def get_all_tracked_objects(klass):
        tracked_objects = {}
        for tracked_type in klass.get_tracked_object_types().values():
            tracked_objects.update(tracked_type.get_all())
        return tracked_objects

    @property
    def doc_file_name(self):
        return os.path.join(self.documentation_path, self.name + '.json')

    @property
    def description(self):
        return self.documentation['description']

    @description.setter
    def description(self, descript):
        self.documentation['description'] = descript

    @property
    def code_hash(self):
        return hashlib.md5(
            inspect.getsource(type(self)).encode()
        ).hexdigest()

    def _set_up_connection(self):
        self._connection = psycopg2.connect(
            host=self.host,
            database=self.db,
            application_name=self.application_name,
        )
        self._cursor = None

    def _set_up_cursor(self, name=None):
        self._cursor = self._connection.cursor(name)

        # Editing search_path is the best way to set the current schema in PostgreSQL
        # Note that this change will only affect the current connection
        self._cursor.execute("SET search_path TO {schema}".format(schema=self.schema))

        self._cursor.execute('SET ROLE "{common_user}"'.format(common_user=self.common_user))

    def get_cursor(self, name=None):
        """
            Create a new cursor that will be stored and used for all operations we perform.

            This cursor has three important features:

                1) We set the current schema for this cursor so we always
                   create tables in the same schema regardless of who runs
                   the command

                2) We set the current user to the common user so that all
                   tables created by any auto_doc user can be accessed with
                   full permissions by all other auto_doc users
        """
        if not self._connection or self._connection.closed:
            self._set_up_connection()

        if not self._cursor or self._cursor.closed or (self._cursor.name and self._cursor.query):
            self._set_up_cursor(name)

        return self._cursor

    def get_dependencies_for_current_transaction(self, cursor=None, tracked_objects=None):
        # Look up what tables were locked by self.build()
        # These will be our dependencies.
        if not cursor:
            cursor = self.get_cursor()

        pid = cursor.connection.get_backend_pid()

        cursor.execute(
            "SELECT relname "
            "FROM pg_locks JOIN pg_class "
            "ON pg_locks.relation=pg_class.oid "
            "WHERE pg_locks.pid = {pid}".format(pid=pid)
        )

        # Only add documented tables to dependencies
        if not tracked_objects:
            tracked_objects = self.get_all_tracked_objects()
        return list(set(
            x[0]
            for x in cursor.fetchall()
            if x[0] in tracked_objects and x[0] != self.name
        ))

    def create_code_tracking_table(self, cursor=None):
        if not cursor:
            cursor = self.get_cursor()
        cursor.execute(
            "CREATE TABLE IF NOT EXISTS {code_tracker} (table_name TEXT UNIQUE, code_hash TEXT)".format(
                code_tracker=self.code_tracking_table_name,
            )
        )

    def update_code_tracking_table(self, cursor=None):
        # Make a hash from the text of our build() function so we can tell if the code has changed since the table
        # was last built
        if not cursor:
            cursor = self.get_cursor()

        self.create_code_tracking_table(cursor=cursor)
        cursor.execute(
            "INSERT INTO {code_tracker} VALUES ('{table_name}', '{code_hash}') "
            "ON CONFLICT (table_name) DO UPDATE SET code_hash = '{code_hash}' WHERE {code_tracker}.table_name = '{table_name}'".format(
                code_tracker=self.code_tracking_table_name,
                table_name=self.name,
                code_hash=self.code_hash,
            )
        )

    def get_dependencies(self, tracked_objects=None, cursor=None):
        """
        Recursively get all dependencies and report if they
        exist / are changed from their documentation.

        Also check there are no circular dependencies.

        Return a list of dictionaries that report this
        info for each dependency.
        """

        if not hasattr(self, '_dependencies'):
            self._dependencies = self._recursive_get(
                table_list_key='dependencies',
                tracked_objects=tracked_objects,
                cursor=cursor,
            )
        return self._dependencies

    def get_descendents(self, tracked_objects=None, cursor=None):
        """
        Recursively get a list of all descendents of this table.
        That is, all tables that list this table as one of their dependencies.

        Return format matches get_dependencies
        """

        if not hasattr(self, '_descendents'):
            self._descendents = self._recursive_get(
                table_list_key='descendents',
                tracked_objects=tracked_objects,
                cursor=cursor,
            )
        return self._descendents

    def _recursive_get(
        self,
        table_list_key,
        cur_table_path=[],
        full_table_path=[],
        level=0,
        tracked_objects=None,
        cursor=None,
    ):
        """
        The meat of the code for both get_dependencies and get_descendents.

        Recursively traverses through the tables attached to this table
        on an arbitrary attribute.

        said attribute (eg. 'dependencies' or 'descendents') must contain
        a list of tables, and must be present on all tables.
        """
        if not cursor:
            self_loaded_cursor = True
            cursor = self.get_cursor()
        else:
            self_loaded_cursor = False

        if not tracked_objects:
            tracked_objects = self.get_all_tracked_objects()
        table_list = [
            tracked_objects[key]
            for key in self.documentation[table_list_key]
            if key in tracked_objects
        ]

        for table in table_list:

            circular_dependency_msg = (
                "Circular dependency detected. {path}".format(
                    path=str.join("<-", cur_table_path) + "<-" + table.name,
                )
            )
            assert table.name not in cur_table_path, circular_dependency_msg

            table.update_snapshot(cursor=cursor)

            table_info = {
                "name": table.name,
                "exists": table.exists,
            	"ready": table.exists and not table.changed,
            	"paths": cur_table_path,
            }

            full_table_path = table._recursive_get(
                table_list_key=table_list_key,
                cur_table_path=cur_table_path+[table.name], 
                full_table_path=full_table_path+[table_info],
                level=level+1,
                cursor=cursor,
                tracked_objects=tracked_objects,
            )

        if self_loaded_cursor:
            self.close_connection()

        return full_table_path

    def set_up(self, cursor=None):
        if not cursor:
            cursor = self.get_cursor()

        self.update_snapshot(cursor=cursor)

        if self.changed:
            self.tear_down(cursor=cursor)

        if not self.exists:
            cursor.execute("SAVEPOINT {save_point_prefix}_{self}".format(
                save_point_prefix=self.save_point_prefix,
                self=self.name,
            ))

            try:
                self.build(cursor=cursor)
            except Exception as e:
                cursor.execute("ROLLBACK TO SAVEPOINT {save_point_prefix}_{self}".format(
                    save_point_prefix=self.save_point_prefix,
                    self=self.name,
                ))
                self.is_updated = False
                raise e
            else:
                self._db_snapshot['dependencies'] = self.get_dependencies_for_current_transaction(cursor=cursor)
                self.update_code_tracking_table(cursor=cursor)
                self.is_updated = False
            finally:
                cursor.execute("RELEASE SAVEPOINT {save_point_prefix}_{self}".format(
                    save_point_prefix=self.save_point_prefix,
                    self=self.name,
                ))

    def tear_down(self, cursor=None):
        if not cursor:
            cursor = self.get_cursor()
        cursor.execute("DROP TABLE IF EXISTS {self}".format(self=self.name))
        self.create_code_tracking_table(cursor=cursor)
        cursor.execute("DELETE FROM {code_tracker} WHERE table_name = '{self}'".format(
                code_tracker=self.code_tracking_table_name,
                self=self.name,
            )
        )

        # If _db_snapshot is up to date, keep it that way
        if self.is_updated:
            self._db_snapshot.update(
                {
                    key: value
                    for key, value in self.empty_documentation.items()
                    if key in self._db_snapshot
                }
            )
            self.update_snapshot(cursor=cursor, query_mode='STOP')

    def close_connection(self):
        if self._cursor is not None:
            self._cursor.close()
            self._cursor = None
        if self._connection is not None:
            self._connection.close()
            self._connection = None

    def finalize(self, cursor=None):
        """
        Grant privileges on our new table to PUBLIC, create an index for each
        column from self.auto_index_columns
        """
        if not cursor:
            cursor = self.get_cursor()
        self.update_snapshot(cursor=cursor)

        if self.exists:
            cursor.execute("GRANT ALL PRIVILEGES ON TABLE {self} TO PUBLIC".format(self=self.name))

            # Get all indexes that apply to this table
            cursor.execute(
                "SELECT indexdef FROM pg_indexes "
                "WHERE tablename = '{self}'".format(
                    self=self.name,
                )
            )
            indexdefs = cursor.fetchall()

            # and figure out which columns they apply to
            already_indexed_columns = set()
            for indexdef, in indexdefs:
                for column in reversed(sorted(self._db_snapshot['column_data'].keys(), key=len)):
                    if column in indexdef:
                        already_indexed_columns.add(column)
                        break

            # check which columns should be indexed
            auto_index_columns = (
                set(
                    self._db_snapshot['column_data'].keys()
                ).intersection(
                    self.auto_index_columns
                ).difference(
                    already_indexed_columns
                )
            )

            # Only add an index if it doesn't already exist
            for column in auto_index_columns:
                cursor.execute(
                    "CREATE INDEX {self}_{column}_idx ON {schema}.{self} ({column})".format(
                        schema=self.schema,
                        self=self.name,
                        column=column,
                    )
                )

        cursor.connection.commit()
        self.close_connection()

    def delete_documentation_file(self):
        if os.path.exists(self.doc_file_name):
            os.remove(self.doc_file_name)

    def update_documentation_file(self, insert_self_into_descendents=True, tracked_objects=None):
        self.documentation.update(self._db_snapshot)
        self.delete_documentation_file()

        if not tracked_objects:
            tracked_objects = self.get_all_tracked_objects()

        if self.documentation:
            with open(self.doc_file_name, 'w') as json_doc_file:
                json.dump({self.name: self.documentation}, json_doc_file, indent=4, sort_keys=True)

        if insert_self_into_descendents:
            for dependency_name in self.documentation['dependencies']:
                dependency = tracked_objects[dependency_name]
                if self.name not in dependency.documentation['descendents']:
                    dependency.documentation['descendents'].append(self.name)
                    dependency.update_documentation_file(
                        insert_self_into_descendents=False,
                        tracked_objects=tracked_objects,
                    )

    def build(self, cursor=None):
        """
        This function should be over-ridden by subclasses of subclasses and
        contain code to create a specific object.
        """
        raise NotImplementedError

    def update_snapshot(self, cursor=None, query_mode=None, low_cardinality_columns=None):
        """
        self._db_snapshot is a dictionary that contains the technical parts of documentation
        as reported by the database itself.
        This function populates 'table_data', 'column_data', and
        'table_bytes' as follows:

        {
            'table_data': [[Data from information_schema.tables]],
            'columns': [[List of column names]],
            'table_bytes': [[result of pg_total_relation_size()]],
            'code_hash': [[The code hash from the specially code_tracker]],
        }

        query_mode is a string or None. It can take the value 'FORCE', or 'STOP'. If it's force, then a query
        will be preformed to update _db_snapshot even if self.is_updated == True. If it's stop, then no query
        will be performed even if self.is_updated == False.

        'description' and 'dependencies' are not in self._db_snapshot, as they don't exist in the database.

        returns a boolean that says whether or not the function performed any queries.
        """

        should_query = query_mode == 'FORCE' or (query_mode != 'STOP' and not self.is_updated)

        if should_query:
            if not cursor:
                cursor = self.get_cursor()

            if not low_cardinality_columns:
                low_cardinality_columns = set()

            # Look in the information schema for a table with our name.
            cursor.execute(
                "SELECT * FROM information_schema.tables WHERE table_name = '{self}' AND table_schema = '{schema}'".format(
                    self=self.name,
                    schema=self.schema,
                )
            )

            data_descriptions = cursor.description
            table_data = cursor.fetchone() or []

            self._db_snapshot['table_data'] = {
                descriptor.name: datum
                for descriptor, datum in zip(data_descriptions, table_data)
            }

        self.exists = bool(self._db_snapshot.get('table_data', {}))

        if should_query and self.exists:
            # Get the columns for our table
            cursor.execute(
                "SELECT * FROM information_schema.columns WHERE table_name = '{self}' AND table_schema = '{schema}'".format(
                    self=self.name,
                    schema=self.schema,
                )
            )
            data_descriptions = cursor.description
            column_data_sets = cursor.fetchall()

            self._db_snapshot['column_data'] = {}
            for column_data in column_data_sets:
                data_dict = {
                    descriptor.name: datum
                    for descriptor, datum in zip(data_descriptions, column_data)
                }
                self._db_snapshot['column_data'][data_dict['column_name']] = data_dict

            # Change statistics length for all columns in this table to make ANALYZE faster
            # And make the contents of pg_stats more storable and human readable.
            for column in self._db_snapshot['column_data'].keys():
                cursor.execute(
                    "ALTER TABLE {self} ALTER {column} SET STATISTICS {coded_column_cap}".format(
                        self=self.name,
                        column=column,
                        coded_column_cap=self.coded_column_cap,
                    )
                )

            cursor.execute("ANALYZE {self}".format(self=self.name))
            cursor.execute(
                "SELECT * "
                "FROM pg_stats WHERE tablename = '{self}' AND schemaname = '{schema}'".format(
                    self=self.name,
                    schema=self.schema,
                )
            )
            stat_descriptions = cursor.description
            column_stat_sets = cursor.fetchall()

            self._db_snapshot['column_stats'] = {}
            low_cardinality_columns |= self.update_column_stats(stat_descriptions, column_stat_sets)

            # Remove any columns without stats from consideration
            # This should only impact columns that were passed in but no longer exist
            low_cardinality_columns = low_cardinality_columns.intersection(set(self._db_snapshot['column_stats'].keys()))

            if low_cardinality_columns:
                # Up STATISTICS for columns with low cardinality so we can be more confident in n_distinct
                # then re-analyzere-analyze the table and get updated stats

                for column in low_cardinality_columns:
                    cursor.execute(
                        "ALTER TABLE {self} ALTER {column} SET STATISTICS {quick_analyze_limit}".format(
                            self=self.name,
                            column=column,
                            quick_analyze_limit=self.quick_analyze_limit,
                        )
                    )

                cursor.execute(
                    "ANALYZE {self}({columns})".format(
                        self=self.name,
                        columns=str.join(
                            ', ',
                            low_cardinality_columns,
                        ),
                    )
                )
                cursor.execute(
                    "SELECT * "
                    "FROM pg_stats "
                    "WHERE "
                        "tablename = '{self}' AND "
                        "schemaname = '{schema}' AND "
                        "attname IN ('{columns}')".format(
                        self=self.name,
                        schema=self.schema,
                        columns=str.join(
                            "', '",
                            low_cardinality_columns,
                        ),
                    )
                )
                stat_descriptions = cursor.description
                column_stat_sets = cursor.fetchall()

                self.update_column_stats(stat_descriptions, column_stat_sets)

            # Get table size in bytes
            # Much faster to get than row count, and more consistent than
            # any approximate row count, but will still tell us if row count
            # has changed between builds
            cursor.execute("SELECT pg_table_size('{schema}.{self}')".format(schema=self.schema, self=self.name))
            self._db_snapshot['table_bytes'] = cursor.fetchone()[0]

            # Ensure the code tracking table exists by creating an empty table if it doesn't 
            self.create_code_tracking_table(cursor=cursor)

            # Then fetch whatever code hash is stored in the database for this table
            cursor.execute("SELECT code_hash FROM {code_tracker} WHERE table_name = '{self}'".format(
                    code_tracker=self.code_tracking_table_name,
                    self=self.name,
                )
            )
            self._db_snapshot['code_hash'] = cursor.fetchone()
            if self._db_snapshot['code_hash']:
                self._db_snapshot['code_hash'] = self._db_snapshot['code_hash'][0]
            else:
                self._db_snapshot['code_hash'] = ''

        if not self.exists:
            self._db_snapshot['table_data'] = {}
            self._db_snapshot['column_data'] = {}
            self._db_snapshot['column_stats'] = {}
            self._db_snapshot['table_bytes'] = 0
            self._db_snapshot['code_hash'] = ''

        # define intermediate changed variables so we can tell exactly what was changed
        self.table_changed = self.documentation['table_data'] != self._db_snapshot['table_data']
        self.column_changed = self.documentation['column_data'] != self._db_snapshot['column_data']
        self.size_changed = self.documentation['table_bytes'] != self._db_snapshot['table_bytes']
        self.code_changed = not (self.documentation['code_hash'] == self._db_snapshot['code_hash'] == self.code_hash)

        self.changed = self.size_changed or self.column_changed or self.table_changed or self.code_changed
        self.is_updated = True

    def update_column_stats(self, stat_descriptions, column_stat_sets):
        low_cardinality_columns = set()

        for column_stats in column_stat_sets:
            stat_dict = {
                descriptor.name: stat
                for descriptor, stat in zip(stat_descriptions, column_stats)
            }

            stat_dict['most_common_vals'] = list(self.most_common_vals_to_set(
                stat_dict['most_common_vals']
            ))
            stat_dict['histogram_bounds'] = list(self.most_common_vals_to_set(
                stat_dict['histogram_bounds']
            ))
            for key, stat in stat_dict.items():
                if isinstance(stat, list):
                    stat_dict[key] = stat[:self.coded_column_cap]

            self._db_snapshot['column_stats'].setdefault(stat_dict['attname'], {})
            self._db_snapshot['column_stats'][stat_dict['attname']].update(stat_dict)

            # If any colums come out with low cardinality, crank up STATISTICS for that column to confirm
            for column, stats in self._db_snapshot['column_stats'].items():
                if 0 < float(stats['n_distinct']) <= self.coded_column_cap:
                    low_cardinality_columns.add(column)

        return low_cardinality_columns

class TrackedColumn(TrackedObject):

    object_type = 'columns'
    buildable = False

    empty_documentation = out_of_place_update(
        TrackedObject.empty_documentation, {
            'home_table': '',
            'column_stats': {},
            'decode_values': {},
        }
    )

    @classmethod
    def create_dummy_subclass(klass, name):
        return TrackedProperties(name, (klass,), {'name': name})

    @classmethod
    def create_dummy_instance(klass, name, documentation={}):
        return klass.create_dummy_subclass(name)(documentation)

    @classmethod
    def get_all(klass):
        """
        Tracked Columns are a much more minimal class than TrackedTables,
        existing basically to supliment that class rather than stand on their own.

        There is no code that should be unique to a specific TrackedColumn
        the way the build() function is unique to a specific tracked table.

        As a result, we don't really need users to create files that define
        subclasses for TrackedColumn just to define a name for each. Instead,
        we can just create these subclasses programatically.
        """
        documentation = klass.load_documentation()

        tracked_objects = {}
        for name, data in documentation.items():
            tracked_objects[name] = klass.create_dummy_instance(name, data)

        return tracked_objects

    @property
    def home_table(self):
        return self.documentation.get('home_table', None)

    @home_table.setter
    def home_table(self, value):
        self.documentation['home_table'] = value

    @property
    def is_traveling(self):
        return self.home_table != getattr(self, 'current_table', self.home_table)

    @property
    def is_coded(self):
        """
        If this column has documentation for decode_values, then it's a coded column, so return True.
        If pg_stats.n_distinct is between 0 and coded_column_cap it may be a coded column, so return None.
        Otherwise, return False.
        """
        if self.documentation['decode_values']:
            return True
        if self.is_updated and self._db_snapshot['column_stats']:
            if 0 < float(self._db_snapshot['column_stats']['n_distinct']) >= self.coded_column_cap:
                return None
        elif self.documentation['column_stats']:
            if 0 < float(self.documentation['column_stats']['n_distinct']) >= self.coded_column_cap:
                return None
        else:
            return False

    @property
    def possible_values(self):
        """
        Returns most_common_values from pg_stats if it's available.
        Note that if self.is_coded is True, then this will
        include all possible_values as estimated by pg_stats because 
        TrackedObject.finalize() sets STATISTICS = coded_column_cap.
        """
        possible_values = set(self.documentation['decode_values'].keys())

        if self.is_updated and self._db_snapshot['column_stats']:
            if self._db_snapshot['column_stats']['most_common_vals']:
                possible_values = possible_values.union(
                    set(self._db_snapshot['column_stats']['most_common_vals'])
                )

        if self.documentation['column_stats']:
            if self.documentation['column_stats']['most_common_vals']:
                possible_values = possible_values.union(
                    set(self.documentation['column_stats']['most_common_vals'])
                )

        return possible_values

    def fit_name_to_convention(self, test_name=None):
        if test_name is None:
            test_name = self.name
        if not self.name_fits_convention(test_name):
            return "{table_name}_{column_name}".format(
                table_name=self.home_table,
                column_name=test_name,
            )
        else:
            return test_name

    def name_fits_convention(self, test_name=None):
        if test_name is None:
            test_name = self.name
        return test_name.startswith(
            "{home_table}_".format(
                home_table=self.home_table
            )
        )

    def update_snapshot(self, column_data=None, column_stats=None):
        """
        Tracked columns should never do their own queries because its more efficient to do them at the table level.
        Therfore, this function just gets told what to do.
        """

        self._db_snapshot['column_data'] = {}
        self._db_snapshot['column_stats'] = {}

        initial_possible_values = self.possible_values

        if column_data:
            self._db_snapshot['column_data'] = column_data
        if column_stats:
            self._db_snapshot['column_stats'] = column_stats

        self.exists = bool(column_data)
        self.is_updated = True

        # Changes in columns first created in other tables and merely ported to this one should be ignored.
        # They will be on the level of a cropped sample size if they exist and don't need to be recorded anywhere.
        if not self.is_traveling:
            self.column_changed = self._db_snapshot['column_data'] != self.documentation['column_data']
            if self.is_coded is not False:
                self.values_changed = bool(self.possible_values - initial_possible_values)
            else:
                self.values_changed = False

            self.changed = self.column_changed or (self.is_coded and self.values_changed)
        else:
            self.column_changed = self.values_changed = self.changed = not self.exists

    def delete_documentation_file(self, *args, **kwargs):
        if not self.is_traveling:
            TrackedObject.delete_documentation_file(self, *args, **kwargs)

    def update_documentation_file(self, *args, **kwargs):
        if not self.is_traveling:
            TrackedObject.update_documentation_file(self, *args, **kwargs)

    def set_up(self):
        raise NotImplementedError("Columns cannot be built. Build the table they belong to instead.")

    def tear_down(self):
        raise NotImplementedError("Columns cannot be torn down. Tear down the table they belong to instead.")

    def finalize(self):
        raise NotImplementedError("Columns cannot be finalized. Finalize the table they belong to instead.")


class CommonTable():
    """
    Lots of TrackedObjects are going to be tables with columns in the database, but represent different kinds of
    objects outside of it. For instance, TrackedTable is any table that a researcher has built from raw data,
    and FixedWidthImportTable is a table built directly from by importing a fixed width file (or files).

    This class defines all the functionality objects represented by tables in the database have in common.
    """

    empty_documentation = out_of_place_update(
        TrackedObject.empty_documentation, {
            'renamed_columns': {},
        }
    )

    _columns = {}

    @property
    def columns(self):
        if not hasattr(self, '_tracked_columns'):
            self._tracked_columns = TrackedColumn.get_all()

        existing_column_names = set(self._columns.keys())

        doc_column_names = set(
            self.documentation['column_data'].keys()
        ).union(
            column.name
            for column in self._tracked_columns.values()
            if column.home_table == self.name
        )

        snapshot_column_names = set(
            getattr(
                self,
                '_db_snapshot',
                {},
            ).get(
                'column_data',
                {},
            ).keys()
        )

        self._columns = {
            name: (
                self._columns.get(name) or
                deepcopy(self._tracked_columns.get(name)) or
                TrackedColumn.create_dummy_instance(
                    name,
                )
            )
            for name in existing_column_names.union(
                doc_column_names
            ).union(
                snapshot_column_names
            )
        }

        table_names = list(self.get_all().keys())
        # Sort table names with longest ones first so that when we iterate
        # through this list we know the first one the column name starts
        # with is the best match
        table_names.sort(key=len)
        table_names.reverse()
        for column in self._columns.values():
            if not column.home_table:
                for table_name in table_names:
                    if column.name.startswith(table_name):
                        column.home_table = table_name
                        break
                else:
                    column.home_table = self.name
            column.current_table = self.name

        return self._columns

    @property
    def extant_columns(self):
        return {
            column_name: column 
            for column_name, column in self.columns.items() 
            if column.exists
        }

    def __getitem__(self, key):
        return self._columns[key]

    def __setitem__(self, key, value):
        if not isinstance(value, TrackedColumn):
            raise ValueError("Tables can only contain objects of type TrackedColumn, not {type}".format(type=type(value)))
        self._columns[key] = value

    def change_column_name_in_database(self, current_column_name, new_column_name, cursor=None):
        if not cursor:
            cursor = self.get_cursor()
        cursor.execute(
            "ALTER TABLE {self} "
            "RENAME COLUMN {current_column_name} TO {new_column_name}".format(
                self=self.name,
                current_column_name=current_column_name,
                new_column_name=new_column_name,
            )
        )

        # If self._db_snapshot is up to date, keep it that way
        if self.is_updated:
            self._db_snapshot['column_data'][new_column_name] = self._db_snapshot['column_data'].pop(current_column_name)
            self.update_snapshot(cursor=cursor, query_mode='STOP')

    def rename_column(self, current_column_name, new_column_name):
        self.documentation['renamed_columns'][current_column_name] = new_column_name

        old_column = deepcopy(self.columns.get(current_column_name))
        if new_column_name in self._columns:
            for key, value in self._columns[new_column_name].documentation.items():
                if value:
                    old_column.documentation[key] = value

        self._columns[new_column_name] = old_column
        self._columns[new_column_name].name = new_column_name

    def update_snapshot(self, cursor=None, query_mode=None):
        if not cursor:
            cursor=self.get_cursor()

        TrackedObject.update_snapshot(
            self,
            cursor=cursor,
            query_mode=query_mode,
            low_cardinality_columns=set(
                column.name
                for column in self.columns.values()
                if column.is_coded is True
            ),
        )

        self._db_snapshot.setdefault('column_data', {})
        self._db_snapshot.setdefault('column_stats', {})
        for column in self.columns.values():
            column.update_snapshot(
                column_data=self._db_snapshot['column_data'].get(
                    column.name,
                    [],
                ),
                column_stats=self._db_snapshot['column_stats'].get(
                    column.name,
                    [],
                ),
            )

        self.column_changed = self.column_changed or any(
            column.changed
            for column in self.columns.values()
        )
        self.changed = self.changed or self.column_changed

    def finalize(self, cursor=None):
        """
        Rename all columns that need renaming, and then update documentation.

        Documentation goes last so errors there don't prevent the database
        operations from completing.
        """
        if not cursor:
            cursor = self.get_cursor()
        self.update_snapshot(cursor=cursor)

        for original_name, new_name in self.documentation['renamed_columns'].items():
            if original_name in self.columns and self.columns[original_name].exists:
                self.change_column_name_in_database(
                    original_name,
                    new_name,
                    cursor=cursor,
                )

        TrackedObject.finalize(self, cursor=cursor)

    def update_documentation_file(self, insert_self_into_descendents=True, tracked_objects=None):
        if not tracked_objects:
            tracked_objects = self.get_all_tracked_objects()

        TrackedObject.update_documentation_file(
            self, 
            insert_self_into_descendents=insert_self_into_descendents,
            tracked_objects=tracked_objects,
        )

        for column in self.columns.values():
            if column.home_table == self.name:
                column.update_documentation_file(
                    insert_self_into_descendents=insert_self_into_descendents,
                    tracked_objects=tracked_objects,
                )


class TrackedTable(CommonTable, TrackedObject):
    object_type = 'tables'
    prefix = ''

    code_template = (
        "from {package} import {klass}\n"
        "\n"
        "\n"
        "class {table_class_name}({klass}):\n"
        "    name = '{table_name}'\n"
        "\n"
        "    def build(self, cursor=None):\n"
        "        # Put code that builds this table here.\n"
        "        # don't close the cursor when you're done, that's done automatically.\n"
        "        if not cursor:\n"
        "            cursor = self.get_cursor()\n"
    )


class TrackedGraph(TrackedObject):

    object_type = 'graphs'
    prefix = 'graph'

    code_template = (
        "from {package} import {klass}\n"
        "\n"
        "\n"
        "class {table_class_name}({klass}):\n"
        "    name = '{table_name}'\n"
        "\n"
        "    def build(self, cursor=None):\n"
        "        # Put code that builds this table here.\n"
        "        # don't close the cursor when you're done, that's done automatically.\n"
        "        if not cursor:"
        "            cursor = self.get_cursor()"
        "\n"
        "    def make_graph_product(self):\n"
        "        # Use data_frame = self.load_dataframe() to load this table as a dataframe\n"
        "        # and create a csv for our records.\n"
        "\n"
        "        # Use self.save_plot(name) to save and close a plot created with data_frame.plot\n"
        "        # name should be just the name of the file, no path or file extension\n"
        "        pass\n"
    )

    @property
    def graph_product_path(self):
        return os.path.join(
            self.subclass_file_path,
            'graph_product',
            self.name,
        )

    @property
    def csv_output_path(self):
        return os.path.join(self.graph_product_path, '{}.csv'.format(self.name))

    def finalize(self, cursor=None):
        if not cursor:
            cursor=self.get_cursor()

        super().finalize(cursor=cursor)
        try:
            self.make_graph_product()
        except Exception as e:
            warnings.warn("Supressing error in make_graph_product: {err}".format(err=str(e)))

    def make_graph_product(self):
        raise NotImplementedError(
            "Each individual graph should overwrite this function with code "
            "that uses load_dataframe and save_plot to create graphs."
        )

    def load_dataframe(self, cursor=None, **kwargs):
        if not cursor:
            cursor = self.get_cursor()

        Path(self.graph_product_path).mkdir(exist_ok=True)

        with open(self.csv_output_path, 'w') as f:
            cursor.copy_expert("COPY {table_name} TO STDOUT CSV HEADER".format(table_name=self.name), f)

        return pandas.read_csv(self.csv_output_path, **kwargs)

    def save_plot(self, name):

        Path(self.graph_product_path).mkdir(exist_ok=True)

        pyplot.savefig(os.path.join(self.graph_product_path, '{}.pdf'.format(name)))
        pyplot.close()


class FixedWidthImportTable(CommonTable, TrackedObject):
    object_type = 'fixed_width_imports'
    prefix = 'import'

    flat_file_paths = {}
    meta_column_name = None
    config_path = None
    _config = None
    encoding = 'UTF8' # This string is sent to PostgreSQL. 
    # See https://www.postgresql.org/docs/11/multibyte.html for details on supported character sets.

    code_template = (
        "from {package} import {klass}\n"
        "\n"
        "\n"
        "class {table_class_name}({klass}):\n"
        "    name = '{table_name}'\n"
        "    flat_file_paths = dict() # The path(s) to the data file(s) (.dat.gz file)\n"
        "                             # This is dictionary where the key is the value \n"
        "                             # of the meta_column_name for data from this file.\n"
        "\n"
        "    meta_column_name = None # The name of the column that will be added to\n"
        "                            # the data to explain which flat file it came \n"
        "                            # from. If None, no extra column will be added.\n"
        "\n"
        "    # Fixed Width Import tables also require you create a config file at \n"
        "    # {config_path}\n"
        "    # Named {table_name}.json"
    )

    def _set_up_connection(self):
        TrackedObject._set_up_connection(self)
        self._connection.set_client_encoding(self.encoding)

    @property
    def full_meta_column_name(self):
        if self.meta_column_name:
            if self.meta_column_name.startswith(self.name + '_'):
                return self.meta_column_name
            else:
                return self.name + '_' + self.meta_column_name
        else:
            return None

    def build(self, cursor=None):
        if not cursor:
            cursor = self.get_cursor()

        if self.full_meta_column_name:
            meta_column_line = ", {meta_column_name} VARCHAR({length})".format(
                meta_column_name=self.full_meta_column_name,
                length=max([
                    len(meta_column_value)
                    for meta_column_value in sorted(self.flat_file_paths.keys())
                ]),
            )
        else:
            meta_column_line = " "

        cursor.execute(
            "CREATE TABLE {table_name} ("
                "{column_definitons}"
                "{meta_column_line}"
            ")".format(
                table_name=self.name,
                column_definitons=str.join(', ', [
                    "{column_name} VARCHAR ({length})".format(
                        column_name=column_name,
                        length=column_data['length'] + 1,
                    )
                    for column_name, column_data in self.config.items()
                ]),
                meta_column_line=meta_column_line,
            )
        )

        for meta_column_value, flat_file_path in sorted(self.flat_file_paths.items()):
            cursor.execute(
                "DROP TABLE IF EXISTS temp_{name};"
                "CREATE TABLE temp_{name} (data TEXT)".format(
                    name=self.name
                )
            )

            if hasattr(settings, 'c_drive_folder'):
                c_drive_file_path = os.path.join(
                    settings.c_drive_folder,
                    os.path.basename(flat_file_path),
                )

                if not os.path.exists(c_drive_file_path):
                    shutil.copyfile(
                        flat_file_path,
                        c_drive_file_path,
                    )
                flat_file_path = c_drive_file_path

            with gzip.open(flat_file_path, 'r') as big_file:
                for i, file_chunk in enumerate(self.read_file_in_chunks(big_file)):
                    try:
                        cursor.copy_expert("COPY temp_{name} FROM STDIN".format(name=self.name), file_chunk)
                    except psycopg2.DataError as e:
                        file_chunk.seek(0)
                        chunk_lines = sum(1 for line in file_chunk)
                        raise Exception(
                            "{type} occured while parsing {flat_file_path}, between line {first_line} and {last_line} (chunk is {x} lines)".format(
                                type=type(e).__name__,
                                flat_file_path=flat_file_path,
                                first_line=chunk_lines*(i-1),
                                last_line=chunk_lines*i,
                                x=chunk_lines,
                            )
                        ) from e

            if self.full_meta_column_name:
                meta_column_line = ", '{meta_column_value}' as {meta_column_name} ".format(
                    meta_column_value=meta_column_value,
                    meta_column_name=self.full_meta_column_name,
                )
            else:
                meta_column_line = " "

            cursor.execute(
                "INSERT INTO {table_name} "
                "SELECT "
                    "{columns}"
                    "{meta_column_line}"
                "FROM temp_{table_name}".format(
                    meta_column_line=meta_column_line,
                    table_name=self.name,
                    columns=str.join(', ', [
                        "TRIM(SUBSTRING(data, {start_pos}, {length})) AS {column_name}".format(
                            column_name=column_name,
                            start_pos=column_data['start_pos'],
                            length=column_data['length'],
                        )
                        for column_name, column_data in self.config.items()
                    ])
                )
            )

            if hasattr(settings, 'c_drive_folder') and os.path.exists(c_drive_file_path):
                os.remove(c_drive_file_path)

    def read_file_in_chunks(self, file_object, lines_in_chunk=100000):
        file_object.readline()
        size_of_line = file_object.tell()
        file_object.seek(0)
        chunk_size = size_of_line * lines_in_chunk
        backslash_escape = bytes.maketrans(b'\\', b' ')

        data = file_object.read(chunk_size)
        while data:
            cur_file_chunk = BytesIO()
            cur_file_chunk.write(data.translate(backslash_escape))
            cur_file_chunk.seek(0)
            yield cur_file_chunk
            cur_file_chunk.close()
            data = file_object.read(chunk_size)

    @classmethod
    def build_code_template(klass, table_name, code_template=None, extra_formating={}):
        if not extra_formating.get('config_path', None):
            extra_formating['config_path'] = klass.get_config_file_path()

        return super().build_code_template(
            table_name,
            code_template=code_template,
            extra_formating=extra_formating,
        )

    @classmethod
    def get_config_file_path(klass):
        """
        config_path:         Should be a valid file path at which to store json config files.
                             These json files are only used by FixedWidthImportTable, and
                             define the layout of a flat file.
        """
        if (
            not hasattr(klass, 'config_path') or
            not klass.config_path
        ):
            return os.path.join(
                klass.subclass_file_path,
                'config_files',
            )
        else:
            return klass.config_path

    @property
    def config_file_name(self):
        return os.path.join(self.get_config_file_path(), getattr(self, 'config_name', None) or self.name + '.json')

    @property
    def config(self):
        if not self._config:
            with open(self.config_file_name, 'r') as f:
                try:
                    self._config = json.load(f, object_pairs_hook=OrderedDict)
                except json.decoder.JSONDecodeError as e:
                    raise json.decoder.JSONDecodeError(
                        "{filename} has json errors".format(
                            filename=self.config_file_name,
                        ),
                        e.doc,
                        e.pos,
                    ) from e

            for key in self._config:
                if 'start_pos' not in self._config[key]:
                    raise AttributeError(
                        "Config for {self}.{key} is missing 'start_pos'".format(
                            self=self.name, key=key,
                        )
                    )

                if 'length' not in self._config[key]:
                    if 'end_pos' in self._config[key]:
                        self._config[key]['length'] = self._config[key]['end_pos'] - self._config[key]['start_pos'] + 1
                    else:
                        raise AttributeError(
                            "Config for {self}.{key} is missing 'length' (or end_pos)".format(
                                self=self.name, key=key,
                            )
                        )

        return self._config
