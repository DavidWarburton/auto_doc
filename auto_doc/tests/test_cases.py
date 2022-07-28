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

import unittest
import os
import psycopg2
import json
import glob
import shutil
import warnings
import csv
import inspect

from unittest import mock
from io import StringIO
from importlib import import_module, reload
from copy import copy, deepcopy
from ..run import (
    read_from_editor,
    get_input,
    get_input_from_editor,
    order_tables_by_dependencies,
    build_tables,
    output_tables,
    list_tables,
    document_tables,
    document_description,
    rename_columns,
    document_columns,
    document_possible_values,
    handle_untracked_input,
    get_tables_to_consider,
    DependecyError,
    TEMP_INPUT_PATH,
    TABLE_DESCRIPTION_TEMPLATE,
    COLUMN_DESCRIPTION_TEMPLATE,
    COLUMN_VALUE_TEMPLATE,
)
from .. import auto_doc
from . import initable_tracked_object


class CommonSetUpTestCase(unittest.TestCase):
    def setUp(self):
        self.tracked_table = auto_doc.TrackedTable
        self.tracked_column = auto_doc.TrackedColumn
        self.tracked_graph = auto_doc.TrackedGraph
        self.tracked_object = auto_doc.TrackedObject
        self.tracked_properties = auto_doc.TrackedProperties
        self.fixed_width_import_table = auto_doc.FixedWidthImportTable
        self.application_name = 'auto_doc_test'
        self.code_tracking_table_name = 'test_code_tracker'

        self.initable_tracked_object = initable_tracked_object.InitableTrackedObject
        self.initable_tracked_object.application_name = self.application_name
        self.initable_tracked_object.code_tracking_table_name = self.code_tracking_table_name

        self.tracked_table.subclass_pack_path = str.join('.', [
            __package__,
            self.tracked_table.object_type,
        ])
        self.tracked_table.subclass_file_path = os.path.join(
            os.path.dirname(__file__),
            self.tracked_table.object_type,
        )
        self.tracked_table.documentation_path = os.path.join(
            self.tracked_table.subclass_file_path,
            'documentation',
        )
        self.tracked_table.auto_index_columns = [
            'column_1',
            'import_test_1_age',
            'import_test_1_file',
            'import_test_1_first_name',
            'import_test_1_last_name',
            'test_table_1_column_1',
            'test_table_1_column_2',
            'test_table_2_column_1',
            'test_table_4_column_1',
            'test_table_5_column_1',
            'test_table_5_column_2',
            'test_table_6_column_1',
            'test_table_7_column_1',
        ]
        self.tracked_table.application_name = self.application_name
        self.tracked_table.code_tracking_table_name = self.code_tracking_table_name

        self.fixed_width_import_table.subclass_pack_path = str.join('.', [
            __package__,
            self.fixed_width_import_table.object_type,
        ])
        self.fixed_width_import_table.subclass_file_path = os.path.join(
            os.path.dirname(__file__),
            self.fixed_width_import_table.object_type,
        )
        self.fixed_width_import_table.documentation_path = os.path.join(
            self.fixed_width_import_table.subclass_file_path,
            'documentation',
        )
        self.fixed_width_import_table.application_name = self.application_name
        self.fixed_width_import_table.code_tracking_table_name = self.code_tracking_table_name

        self.tracked_column.subclass_pack_path = str.join('.', [
            __package__,
            self.tracked_column.object_type,
        ])
        self.tracked_column.subclass_file_path = os.path.join(
            os.path.dirname(__file__),
            self.tracked_column.object_type,
        )
        self.tracked_column.documentation_path = os.path.join(
            self.tracked_column.subclass_file_path,
            'documentation',
        )
        self.tracked_column.application_name = self.application_name
        self.tracked_column.code_tracking_table_name = self.code_tracking_table_name

        self.tracked_graph.subclass_pack_path = str.join('.', [
            __package__,
            self.tracked_graph.object_type,
        ])
        self.tracked_graph.subclass_file_path = os.path.join(
            os.path.dirname(__file__),
            self.tracked_graph.object_type,
        )
        self.tracked_graph.documentation_path = os.path.join(
            self.tracked_graph.subclass_file_path,
            'documentation',
        )
        self.tracked_graph.application_name = self.application_name
        self.tracked_graph.code_tracking_table_name = self.code_tracking_table_name

        self.test_docs = {
            'test_1': 'test_1',
            'test_2': 'test_2',
            'test_3': 'test_3',
            'test_4': 'test_4',
            'test_5': 'test_5',
        }
        self.column_names = [
            'column_1',
            'test_table_1_column_1',
            'test_table_1_column_2',
            'test_table_2_column_1',
            'test_table_4_column_1',
            'test_table_5_column_1',
            'test_table_5_column_2',
            'test_table_6_column_1',
        ]
        self.table_names = [
            'test_table_1',
            'test_table_2',
            'test_table_3',
            'test_table_4',
            'test_table_5',
            'test_table_6',
            'test_table_7',
        ]
        self.object_names = [
            'test_object_1',
            'test_object_2',
            'test_object_3',
            'test_object_4',
        ]
        self.tracked_object_types = [
            self.tracked_table,
            self.initable_tracked_object,
            self.tracked_graph,
            self.fixed_width_import_table,
            self.tracked_column,
        ]

        connection = psycopg2.connect(host=self.tracked_object.host, database=self.tracked_object.db)
        cursor = connection.cursor()

        cursor.execute("SET search_path TO {schema}".format(schema=self.tracked_object.schema))
        cursor.execute('SET ROLE "{common_user}"'.format(common_user=self.tracked_object.common_user))

        cursor.execute("DROP TABLE IF EXISTS {code_tracker}".format(code_tracker=self.code_tracking_table_name))
        cursor.execute(
            "CREATE TABLE {code_tracker} ("
                "table_name TEXT UNIQUE, "
                "code_hash TEXT"
            ")".format(
                code_tracker=self.code_tracking_table_name,
            )
        )

        # Build all database objects, taking care use no function except build to do this
        # This way if there's an error in on\e of those functions, it won't cause all tests to error
        for tracked_klass in reversed(sorted(self.tracked_object_types, key=lambda x: x.object_type)):
            if tracked_klass.buildable:
                # Note that we build in order by table name, so for test objects and test objects alone
                # dependencies must flow up in the order of the object names, and if they cross object_types,
                # in order of the names of those types
                for obj in sorted(tracked_klass.get_all().values(), key=lambda x: x.name):
                    cursor.execute("DROP TABLE IF EXISTS {obj}".format(obj=obj.name))

                    # set obj._connection and obj._cursor so that when obj.get_cursor() is called in obj.build(), no code will be executed.
                    # and these and our current cursor will be used to build obj.
                    obj._connection = connection
                    obj.build(cursor=cursor)

                    cursor.execute("INSERT INTO {code_tracker} VALUES ('{object_name}', '{code_hash}')".format(
                            code_tracker=self.code_tracking_table_name,
                            object_name=obj.name,
                            code_hash=obj.code_hash,
                        )
                    )

        cursor.close()
        connection.commit()

        # Set up initial docs
        for tracked_klass in self.tracked_object_types:
            initial_docs = glob.glob(
                os.path.join(
                    os.path.dirname(__file__),
                    'initial_docs',
                    tracked_klass.object_type,
                    '*',
                )
            )
            for doc_file in initial_docs:
                shutil.copyfile(
                    doc_file,
                    os.path.join(
                        tracked_klass.documentation_path,
                        os.path.basename(doc_file)
                    )
                )

    def tearDown(self):
        connection = psycopg2.connect(host=self.tracked_object.host, database=self.tracked_object.db)
        cursor = connection.cursor()

        # Close all other connections so that if one of the tests had an error that left an open connection
        # that won't lock connections opened by the next test.
        # note that this will close all connections that have been opened from within test cases
        # (and any that happen to have auto_doc_test as their application_name), so errors may ocur when
        # trying to run two tests concurently.
        cursor.execute(
            "SELECT pg_terminate_backend(pid) "
            "FROM pg_stat_activity "
            "WHERE "
                "pid != pg_backend_pid() AND "
                "application_name = '{application_name}'".format(
                application_name=self.application_name,
            )
        )

        cursor.close()
        connection.commit()

        for tracked_klass in self.tracked_object_types:
            stale_doc_files = glob.glob(
                os.path.join(
                    tracked_klass.documentation_path,
                    '*',
                )
            )
            for doc_file in stale_doc_files:
                os.remove(doc_file)

"""
This is a hacky way to update initial_docs. Just uncomment this and run this test
using the pause in the middle when pdb is imported to decide if the new docs are in shape.
(exit() if they are not)
"""

class CreateNew(CommonSetUpTestCase):

    def docs(self):
        for tracked_klass in reversed(sorted(self.tracked_object_types, key=lambda x: x.object_type)):
            if tracked_klass.buildable:
                for obj in sorted(tracked_klass.get_all().values(), key=lambda x: x.name):
                    obj.tear_down()
                    obj.set_up()
                    obj.finalize()
                    obj.update_documentation_file()
        import pdb; pdb.set_trace()

        # Set up initial docs
        for tracked_klass in self.tracked_object_types:
            initial_docs = glob.glob(
                os.path.join(
                    os.path.dirname(__file__),
                    'initial_docs',
                    tracked_klass.object_type,
                    '*',
                )
            )
            for doc_file in initial_docs:
                shutil.copyfile(
                    os.path.join(
                        tracked_klass.documentation_path,
                        os.path.basename(doc_file)
                    ),
                    doc_file,
                )



class TrackedObjectTestCase(CommonSetUpTestCase):

    def test_init(self):
        test = self.initable_tracked_object(self.test_docs, name='dummy')

        self.assertTrue('description' in test.documentation)
        self.assertTrue('dependencies' in test.documentation)
        self.assertTrue('descendents' in test.documentation)

        for key in self.test_docs:
            self.assertEqual(test.documentation[key], self.test_docs[key])

        self.assertEqual(test._db_snapshot, {})
        self.assertFalse(test.exists)
        self.assertFalse(test.changed)
        self.assertTrue(test._connection is None)
        self.assertTrue(test._cursor is None)

    def test_cannot_init_without_name(self):
        try:
            test = self.tracked_object(self.test_docs)
        except NotImplementedError:
            pass
        else:
            raise AssertionError("Tracked object instantiated without name")

    def test_get_object_types(self):
        object_types = self.tracked_object.get_tracked_object_types()

        self.assertTrue(len(object_types) == len(self.tracked_object_types))
        for tracked_object_type in self.tracked_object_types:
            self.assertTrue(object_types[tracked_object_type.object_type] == tracked_object_type)

    def test_load_documentation(self):
        initable_docs = self.initable_tracked_object.load_documentation()

        self.assertTrue(len(initable_docs) == len(self.object_names))
        self.assertTrue(all(
            table_name in initable_docs
            for table_name in self.object_names
        ))

    def test_get_all(self):
        initable_objects = self.initable_tracked_object.get_all()

        self.assertTrue(len(initable_objects) == len(self.object_names))
        self.assertTrue(all(
            table_name in initable_objects
            for table_name in self.object_names
        ))

        for table in initable_objects.values():
            self.assertTrue(isinstance(table, self.initable_tracked_object))

    def test_get_cursor(self):
        objects = self.initable_tracked_object.get_all()
        test_object = objects['test_object_1']
        cursor = test_object.get_cursor()

        self.assertNotEqual(test_object._connection, None)

        cursor.execute("SHOW search_path")
        self.assertEqual(cursor.fetchone()[0], self.tracked_object.schema)
        cursor.execute("SELECT current_user")
        self.assertEqual(cursor.fetchone()[0], self.tracked_object.common_user)

        cursor.close()
        cursor = test_object.get_cursor()

        self.assertFalse(cursor.closed)

        test_object._connection.close()
        cursor = test_object.get_cursor()

        self.assertFalse(test_object._connection.closed)
        self.assertFalse(test_object._cursor.closed)
        test_object.close_connection()

    def test_get_cursor__returns_same_object_on_second_call(self):
        objects = self.initable_tracked_object.get_all()
        test_object = objects['test_object_1']
        cursor1 = test_object.get_cursor()
        cursor2 = test_object.get_cursor()

        self.assertEqual(cursor1, cursor2)

    def test_get_dependencies(self):
        initable_objects = self.initable_tracked_object.get_all()

        tab1_dependencies = initable_objects['test_object_1'].get_dependencies()
        self.assertEqual(len(tab1_dependencies), 0)

        tab2_dependencies = initable_objects['test_object_2'].get_dependencies()
        self.assertEqual(len(tab2_dependencies), 1)
        self.assertEqual(tab2_dependencies[0]['paths'], [])

        tab3_dependencies = initable_objects['test_object_3'].get_dependencies()
        self.assertEqual(len(tab3_dependencies), 1)
        self.assertEqual(tab3_dependencies[0]['paths'], [])

        tab4_dependencies = initable_objects['test_object_4'].get_dependencies()
        self.assertEqual(len(tab4_dependencies), 3)
        for dep in tab4_dependencies:
            if dep['name'] == 'test_object_1':
                self.assertEqual(dep['paths'], ['test_object_2'])
            else:
                self.assertEqual(dep['paths'], [])

    def test_get_descendents(self):
        initable_objects = self.initable_tracked_object.get_all()

        tab1_descendents = initable_objects['test_object_1'].get_descendents()
        self.assertEqual(len(tab1_descendents), 2)
        self.assertEqual(tab1_descendents[0]['paths'], [])
        self.assertEqual(tab1_descendents[1]['paths'], ['test_object_2'])

        tab2_descendents = initable_objects['test_object_2'].get_descendents()
        self.assertEqual(len(tab2_descendents), 1)
        self.assertEqual(tab2_descendents[0]['paths'], [])

        tab3_descendents = initable_objects['test_object_3'].get_descendents()
        self.assertEqual(len(tab3_descendents), 0)

        tab4_descendents = initable_objects['test_object_4'].get_descendents()
        self.assertEqual(len(tab4_descendents), 0)

    def test_get_descendents__closes_connections(self):
        tables = self.initable_tracked_object.get_all_tracked_objects()

        for table in tables.values():
            table.get_descendents()

        cursor = table.get_cursor()
        cursor.execute(
            "SELECT count(*) "
            "FROM pg_stat_activity "
            "WHERE "
                "pid != pg_backend_pid() AND "
                "application_name = '{application_name}'".format(
                application_name=self.application_name,
            )
        )
        self.assertEqual(cursor.fetchone()[0], 0)
        table.close_connection()

    def test_set_up(self):
        initable_objects = self.initable_tracked_object.get_all()

        initable_objects['test_object_4'].tear_down()
        initable_objects['test_object_4']._db_snapshot['dependencies'] = []
        initable_objects['test_object_4'].set_up()

        self.assertEqual(len(initable_objects['test_object_4']._db_snapshot['dependencies']), 2)
        for dependency_name in initable_objects['test_object_4']._db_snapshot['dependencies']:
            self.assertTrue(
                dependency_name in ['test_table_1', 'test_object_2']
            )

        initable_objects['test_object_4'].close_connection()

    def test_set_up__updates_code_tracker(self):
        initable_objects = self.initable_tracked_object.get_all()

        initable_objects['test_object_4'].tear_down()
        initable_objects['test_object_4']._db_snapshot['dependencies'] = []
        initable_objects['test_object_4'].set_up()

        cursor = initable_objects['test_object_4'].get_cursor()
        cursor.execute(
            "SELECT code_hash FROM {code_tracker} WHERE table_name = '{table_name}'".format(
                code_tracker=self.code_tracking_table_name,
                table_name='test_object_4',
            )
        )
        db_code_hash = cursor.fetchone()[0]

        self.assertEqual(db_code_hash, initable_objects['test_object_4'].code_hash)

        initable_objects['test_object_4'].close_connection()

    def test_set_up__updates_code_tracker_no_duplicates(self):
        initable_objects = self.initable_tracked_object.get_all()

        initable_objects['test_object_4'].tear_down()
        initable_objects['test_object_4']._db_snapshot['dependencies'] = []
        initable_objects['test_object_4'].set_up()
        initable_objects['test_object_4'].update_code_tracking_table()

        cursor = initable_objects['test_object_4'].get_cursor()
        cursor.execute(
            "SELECT code_hash FROM {code_tracker} WHERE table_name = '{table_name}'".format(
                code_tracker=self.code_tracking_table_name,
                table_name='test_object_4',
            )
        )
        results = cursor.fetchall()

        self.assertEqual(len(results), 1)

        initable_objects['test_object_4'].close_connection()

    def test_set_up__releases_savepoint(self):
        initable_objects = self.initable_tracked_object.get_all()

        initable_objects['test_object_4'].tear_down()
        initable_objects['test_object_4'].set_up()

        cursor = initable_objects['test_object_4'].get_cursor()
        with self.assertRaises(psycopg2.InternalError):
            cursor.execute("RELEASE SAVEPOINT {save_point_prefix}_{self}".format(
                save_point_prefix=initable_objects['test_object_4'].save_point_prefix,
                self='test_object_4',
            ))

    def test_set_up__rolls_back_broken(self):
        initable_objects = self.initable_tracked_object.get_all()

        def new_build(self, cursor, *args, **kwargs):
            cursor.execute("CREATE TABLE surely_no_one_will_create_a_table_with_this_name (test TEXT)")
            raise Exception

        import types
        initable_objects['test_object_4'].build = types.MethodType(new_build, initable_objects['test_object_4'])
        cursor = initable_objects['test_object_4'].get_cursor()

        initable_objects['test_object_4'].tear_down()
        with self.assertRaises(Exception):
            initable_objects['test_object_4'].set_up(cursor=cursor)

        cursor.execute("SELECT table_name FROM information_schema.tables")
        self.assertFalse(
            any([
                name_tup[0] == "surely_no_one_will_create_a_table_with_this_name"
                for name_tup in cursor.fetchall()
            ])
        )

    def test_update_snapshot__no_change(self):
        objects = self.initable_tracked_object.get_all()

        for table in objects.values():
            table.update_snapshot()
            self.assertTrue(table.exists)
            self.assertFalse(table.changed)
            table.close_connection()

    def test_update_snapshot__changed_table(self):
        objects = self.initable_tracked_object.get_all()

        for table in objects.values():
            table.update_snapshot()
            if table.exists:

                code_changed = table.code_changed

                temp_table_name = table.name + '_temp'
                cursor = table.get_cursor()

                # create a copy of the current table, but as a temporary table
                cursor.execute(
                    "CREATE TEMPORARY TABLE {temp_table} AS ("
                        "SELECT * FROM {table}"
                    ")".format(
                        temp_table=temp_table_name,
                        table=table.name,
                    )
                )
                cursor.execute("DROP TABLE {table}".format(table=table.name))
                cursor.execute("ALTER TABLE {temp_table} RENAME TO {table}".format(
                        temp_table=temp_table_name,
                        table=table.name,
                    )
                )
                # update table schema because temorary tables have their own schemas
                cursor.execute("SELECT table_schema FROM information_schema.tables WHERE table_name = '{table}'".format(
                        table=table.name,
                    )
                )
                new_temp_schema = cursor.fetchone()[0]
                table.schema = new_temp_schema

                table.update_snapshot(cursor=cursor, query_mode='FORCE')
                self.assertTrue(table.exists)
                self.assertTrue(table.table_changed)
                self.assertTrue(table.column_changed) # couldn't find a way to alter table but not columns
                self.assertTrue(table.size_changed) # size too aparently
                self.assertTrue(table.changed)
                
                self.assertEqual(code_changed, table.code_changed)

            table.schema = self.initable_tracked_object.schema
            table.close_connection()

    def test_update_snapshot__changed_size(self):
        objects = self.initable_tracked_object.get_all()

        for table in objects.values():
            table.update_snapshot()
            if table.exists:

                table_changed = table.table_changed
                code_changed = table.code_changed
                column_changed = table.column_changed

                if len(table._db_snapshot['column_data']):
                    cursor = table.get_cursor()

                    cursor.execute(
                        "INSERT INTO {table} "
                        "SELECT t.i "
                        "FROM generate_series(1, 1000) as t(i)".format(
                            table=table.name,
                        )
                    )

                    table.update_snapshot(query_mode='FORCE')
                    self.assertTrue(table.exists)
                    self.assertTrue(table.size_changed)
                    
                    self.assertEqual(table_changed, table.table_changed)
                    self.assertEqual(column_changed, table.column_changed)
                    self.assertEqual(code_changed, table.code_changed)

            table.close_connection()

    def test_update_snapshot__changed_code(self):
        objects = self.initable_tracked_object.get_all()

        for table in objects.values():
            table.update_snapshot()
            if table.exists:

                size_changed = table.size_changed
                column_changed = table.column_changed
                table_changed = table.table_changed

                table.set_up()
                cursor = table.get_cursor()
                cursor.execute("DELETE FROM {code_tracker} WHERE table_name = '{object_name}'".format(
                        code_tracker=self.code_tracking_table_name,
                        object_name=table.name,
                    )
                )
                cursor.execute("INSERT INTO {code_tracker} VALUES ('{object_name}', '{code_hash}')".format(
                        code_tracker=self.code_tracking_table_name,
                        object_name=table.name,
                        code_hash="Not a normal hash",
                    )
                )

                table.update_snapshot(query_mode='FORCE')
                self.assertTrue(table.exists)
                self.assertTrue(table.code_changed)
                self.assertTrue(table.changed)

                self.assertEqual(column_changed, table.column_changed)
                self.assertEqual(size_changed, table.size_changed)
                self.assertEqual(table_changed, table.table_changed)

            table.close_connection()

    def test_update_snapshot__missing_code_tracker(self):
        objects = self.initable_tracked_object.get_all()

        for table in objects.values():
            table.update_snapshot()
            if table.exists:

                size_changed = table.size_changed
                column_changed = table.column_changed
                table_changed = table.table_changed

                table.set_up()

                cursor = table.get_cursor()
                cursor.execute("DROP TABLE {code_tracker}".format(code_tracker=self.code_tracking_table_name))

                table.update_snapshot(query_mode='FORCE')
                self.assertTrue(table.exists)
                self.assertTrue(table.code_changed)
                self.assertTrue(table.changed)

                self.assertEqual(column_changed, table.column_changed)
                self.assertEqual(size_changed, table.size_changed)
                self.assertEqual(table_changed, table.table_changed)

            table.close_connection()

    def test_update_snapshot__changed_columns(self):
        objects = self.initable_tracked_object.get_all()

        for table in objects.values():
            table.update_snapshot()

            if table.exists:
                table_changed = table.table_changed
                # size_changed = table.size_changed no good way to change columns without changing size
                code_changed = table.code_changed

                cursor = table.get_cursor()
                for column in table._db_snapshot['column_data'].keys():
                    cursor.execute(
                        "ALTER TABLE {table} "
                        "ALTER COLUMN {column} TYPE TEXT".format(
                            table=table.name,
                            column=column,
                        )
                    )
                table._connection.commit()

                table.update_snapshot(query_mode='FORCE')
                self.assertTrue(table.exists)
                self.assertTrue(table.column_changed)

                self.assertEqual(table_changed, table.table_changed)
                # self.assertEqual(size_changed, table.size_changed)
                self.assertEqual(code_changed, table.code_changed)

            table.close_connection()

    def test_update_snapshot__dropped_columns(self):
        objects = self.initable_tracked_object.get_all()

        for table in objects.values():
            table.update_snapshot()
            if table.exists:

                cursor = table.get_cursor()
                for column in table._db_snapshot['column_data'].keys():
                    cursor.execute(
                        "ALTER TABLE {table} "
                        "DROP COLUMN {column}".format(
                            table=table.name,
                            column=column,
                        )
                    )
                table._connection.commit()

                table.update_snapshot(query_mode='FORCE')
                self.assertTrue(table.exists)
                self.assertFalse(table.size_changed)
                if table.documentation['column_data'].keys():
                    self.assertTrue(table.column_changed)
                    self.assertTrue(table.changed)
                else:
                    self.assertFalse(table.column_changed)
                    self.assertFalse(table.changed)
                self.assertFalse(table.table_changed)

            table.close_connection()

    def test_update_snapshot__dropped_table(self):
        objects = self.initable_tracked_object.get_all()

        for table in objects.values():
            cursor = table.get_cursor()
            cursor.execute("DROP TABLE {table}".format(table=table.name))
            table._connection.commit()

            table.update_snapshot()
            self.assertFalse(table.exists)
            self.assertTrue(table.size_changed)
            if table.documentation['column_data'].keys():
                self.assertTrue(table.column_changed)
            else:
                self.assertFalse(table.column_changed)
            self.assertTrue(table.table_changed)
            self.assertTrue(table.changed)

            table.close_connection()

    def test_update_snapshot__stats_correct_length(self):
        objects = self.initable_tracked_object.get_all()

        for table in objects.values():
            cursor = table.get_cursor()
            table.update_snapshot(query_mode='FORCE')

            if table.exists:
                previous_coded_states = {}
                for column, stats in table._db_snapshot['column_stats'].items():
                    column_coded = 0 < float(stats['n_distinct']) <= table.coded_column_cap
                    previous_coded_states[column] = column_coded
                    if column_coded:
                        cursor.execute(
                            "INSERT INTO {table} ({column}) "
                            "SELECT t.i "
                            "FROM generate_series(1, 100) as t(i), generate_series(1, 15)".format(
                                table=table.name,
                                column=column,
                            )
                        )

                table.update_snapshot(query_mode='FORCE')
                for _column, stats in table._db_snapshot['column_stats'].items():
                    # Lists in pg_stats take the form of comma delimited strings, so 
                    # length of list = # of commas - 1
                    for stat in stats:
                        if stat.find(',') != -1:
                            self.assertEqual(stat.count(','), table.coded_column_cap - 1)
            table.close_connection()

    def test_update_snapshot__stats_are_json_serializable(self):
        tables = self.tracked_table.get_all()
        all_type_table = tables['test_table_types']

        all_type_table.update_snapshot(query_mode='FORCE')

        for column, stats in all_type_table._db_snapshot['column_stats'].items():
            try:
                json.dumps(stats)
            except Exception as E:
                raise AssertionError(
                    "{column} created non-json serializeable stats {stats}".format(
                        column=column,
                        stats=stats,
                    )
                )

    def test_update_snapshot__is_updated(self):
        initable_objects = self.initable_tracked_object.get_all()

        for object in initable_objects.values():
            connection = psycopg2.connect(host=self.tracked_object.host, database=self.tracked_object.db)
            cursor = connection.cursor()
            cursor.execute(
                "SELECT pid "
                "FROM pg_stat_activity "
                "WHERE "
                    "pid != pg_backend_pid() AND "
                    "application_name = '{application_name}'".format(
                    application_name=self.application_name,
                )
            )
            self.assertEqual(len(cursor.fetchall()), 0)
            connection.close()

            self.assertFalse(object.is_updated)
            object.update_snapshot()

            connection = psycopg2.connect(host=self.tracked_object.host, database=self.tracked_object.db)
            cursor = connection.cursor()
            cursor.execute(
                "SELECT pid "
                "FROM pg_stat_activity "
                "WHERE "
                    "pid != pg_backend_pid() AND "
                    "application_name = '{application_name}'".format(
                    application_name=self.application_name,
                )
            )
            self.assertEqual(len(cursor.fetchall()), 1)
            connection.close()
            object.close_connection()

            object.update_snapshot()
            connection = psycopg2.connect(host=self.tracked_object.host, database=self.tracked_object.db)
            cursor = connection.cursor()
            cursor.execute(
                "SELECT pid "
                "FROM pg_stat_activity "
                "WHERE "
                    "pid != pg_backend_pid() AND "
                    "application_name = '{application_name}'".format(
                    application_name=self.application_name,
                )
            )
            self.assertEqual(len(cursor.fetchall()), 0)
            connection.close()

    def test_update_snapshot__query_mode_stop(self):
        initable_objects = self.initable_tracked_object.get_all()

        for object in initable_objects.values():
            connection = psycopg2.connect(host=self.tracked_object.host, database=self.tracked_object.db)
            cursor = connection.cursor()

            cursor.execute(
                "SELECT pid "
                "FROM pg_stat_activity "
                "WHERE "
                    "pid != pg_backend_pid() AND "
                    "application_name = '{application_name}'".format(
                    application_name=self.application_name,
                )
            )
            self.assertEqual(len(cursor.fetchall()), 0)

            self.assertFalse(object.is_updated)
            object.update_snapshot(query_mode='STOP')

            cursor.execute(
                "SELECT pid "
                "FROM pg_stat_activity "
                "WHERE "
                    "pid != pg_backend_pid() AND "
                    "application_name = '{application_name}'".format(
                    application_name=self.application_name,
                )
            )
            self.assertEqual(len(cursor.fetchall()), 0)
            connection.close()

    def test_update_snapshot__query_mode_force(self):
        initable_objects = self.initable_tracked_object.get_all()

        for object in initable_objects.values():
            connection = psycopg2.connect(host=self.tracked_object.host, database=self.tracked_object.db)
            cursor = connection.cursor()
            cursor.execute(
                "SELECT pid "
                "FROM pg_stat_activity "
                "WHERE "
                    "pid != pg_backend_pid() AND "
                    "application_name = '{application_name}'".format(
                    application_name=self.application_name,
                )
            )
            self.assertEqual(len(cursor.fetchall()), 0)
            connection.close()

            object.is_updated = True
            object.update_snapshot(query_mode='FORCE')

            connection = psycopg2.connect(host=self.tracked_object.host, database=self.tracked_object.db)
            cursor = connection.cursor()
            cursor.execute(
                "SELECT pid "
                "FROM pg_stat_activity "
                "WHERE "
                    "pid != pg_backend_pid() AND "
                    "application_name = '{application_name}'".format(
                    application_name=self.application_name,
                )
            )
            self.assertEqual(len(cursor.fetchall()), 1)
            connection.close()

            object.close_connection()


    def test_tear_down(self):
        initable_objects = self.initable_tracked_object.get_all()

        cursor = initable_objects['test_object_1'].get_cursor()
        cursor.execute("SELECT * FROM information_schema.tables WHERE table_name = 'test_object_1'")
        test_table_1_exists = bool(cursor.fetchone())

        self.assertTrue(test_table_1_exists)

        initable_objects['test_object_1'].tear_down(cursor=cursor)

        with self.assertRaises(psycopg2.ProgrammingError):
            cursor.execute("SELECT * FROM test_object_1")
        initable_objects['test_object_1'].close_connection()

    def test_tear_down__code_tracker(self):
        initable_objects = self.initable_tracked_object.get_all()

        cursor = initable_objects['test_object_1'].get_cursor()
        cursor.execute(
            "SELECT code_hash FROM {code_tracker} WHERE table_name = 'test_object_1'".format(
                code_tracker=self.code_tracking_table_name
            )
        )
        test_table_1_code_tracked = bool(cursor.fetchone())

        self.assertTrue(test_table_1_code_tracked)

        initable_objects['test_object_1'].tear_down(cursor=cursor)

        with self.assertRaises(psycopg2.ProgrammingError):
            cursor.execute("SELECT * FROM test_object_1")
        initable_objects['test_object_1'].close_connection()

    def test_tear_down__works_when_code_tracker_table_deleted(self):
        initable_objects = self.initable_tracked_object.get_all()

        cursor = initable_objects['test_object_1'].get_cursor()
        cursor.execute(
            "DROP TABLE {code_tracker}".format(
                code_tracker=self.code_tracking_table_name
            )
        )

        initable_objects['test_object_1'].tear_down(cursor=cursor)

        with self.assertRaises(psycopg2.ProgrammingError):
            cursor.execute("SELECT * FROM test_object_1")
        initable_objects['test_object_1'].close_connection()

    def test_close_connection(self):
        objects = self.initable_tracked_object.get_all()
        test_object = objects['test_object_1']
        cursor = test_object.get_cursor()
        self.assertFalse(cursor.closed)
        test_object.close_connection()
        self.assertTrue(cursor.closed)

    def test_finalize(self):
        objects = self.initable_tracked_object.get_all()
        test_object = objects['test_object_1']

        cursor = test_object.get_cursor()
        cursor.execute("DROP TABLE IF EXISTS surely_no_one_will_create_a_table_with_this_name")
        test_object._connection.commit()
        test_object.close_connection()

        cursor = test_object.get_cursor()
        cursor.execute(
            "CREATE TABLE surely_no_one_will_create_a_table_with_this_name ("
                "unimportant_column NUMERIC"
            ")"
        )
        test_object.finalize()
        self.assertTrue(cursor.closed)

        cursor = test_object.get_cursor()
        cursor.execute(
            "SELECT * FROM information_schema.tables "
            "WHERE table_name = 'surely_no_one_will_create_a_table_with_this_name'"
        )
        test_table_exists = bool(cursor.fetchone())
        self.assertTrue(test_table_exists)

        cursor.execute("DROP TABLE surely_no_one_will_create_a_table_with_this_name")
        test_object.finalize()

    def test_delete_documentation_file(self):
        initable_objects = self.initable_tracked_object.get_all()
        initable_objects['test_object_1'].delete_documentation_file()

        self.assertFalse(os.path.exists(initable_objects['test_object_1'].doc_file_name))

    def test_update_documentation_file(self):
        initable_objects = self.initable_tracked_object.get_all()

        initable_objects['test_object_1'].documentation['test_attr'] = 'test_attr_value'
        initable_objects['test_object_1'].documentation['dependencies'] = [
            'test_object_2',
        ]

        initable_objects['test_object_1'].update_documentation_file()

        self.assertTrue(os.path.exists(initable_objects['test_object_1'].doc_file_name))
        self.assertTrue(os.path.exists(initable_objects['test_object_2'].doc_file_name))

        with open(initable_objects['test_object_1'].doc_file_name, 'r') as column_1_doc_file:
            column_1_docs = json.load(column_1_doc_file)
        self.assertTrue('test_attr' in column_1_docs['test_object_1'])
        self.assertTrue('dependencies' in column_1_docs['test_object_1'])
        self.assertTrue(
            'test_object_2' in
            column_1_docs['test_object_1']['dependencies']
        )

        with open(initable_objects['test_object_2'].doc_file_name, 'r') as column_2_doc_file:
            column_2_docs = json.load(column_2_doc_file)
        self.assertTrue('descendents' in column_2_docs['test_object_2'])
        self.assertTrue(
            'test_object_1' in
            column_2_docs['test_object_2']['descendents']
        )


class TrackedPropertiesTestCase(CommonSetUpTestCase):

    def test_path_properties__can_be_overridden(self):
        dummy_attrs = {
            'object_type': 'dummy',
            'subclass_file_path': 'dummy',
            'subclass_pack_path': 'dummy',
            'documentation_path': 'dummy',
        }

        DummyClass = self.tracked_properties('DummyClass', (), dummy_attrs)

        self.assertEqual(DummyClass.subclass_pack_path, dummy_attrs['subclass_pack_path'])
        self.assertEqual(DummyClass.subclass_file_path, dummy_attrs['subclass_file_path'])
        self.assertEqual(DummyClass.documentation_path, dummy_attrs['documentation_path'])

    def test_subclass_pack_path(self):
        dummy_attrs = {
            'object_type': 'dummy',
            'subclass_file_path': os.path.join('auto_doc', 'dummy'),
            'documentation_path': 'dummy',
        }

        DummyClass = self.tracked_properties('DummyClass', (), dummy_attrs)

        self.assertEqual(DummyClass.subclass_pack_path, str.join('.', ['auto_doc', dummy_attrs['object_type']]))


    def test_subclass_file_path_builds_from_subclass_pack_path(self):
        dummy_attrs = {
            'object_type': 'dummy',
            'subclass_pack_path': self.tracked_table.subclass_pack_path,
        }

        DummyClass = self.tracked_properties('DummyClass', (), dummy_attrs)

        self.assertEqual(DummyClass.subclass_file_path, self.tracked_table.subclass_file_path)

    def test_documentation_path_builds_from_subclass_file_path(self):
        dummy_attrs = {
            'object_type': 'dummy',
            'subclass_file_path': os.path.join('auto_doc', 'dummy'),
            'subclass_pack_path': 'dummy',
        }

        DummyClass = self.tracked_properties('DummyClass', (), dummy_attrs)

        self.assertTrue(DummyClass.documentation_path.endswith(
            os.path.join(dummy_attrs['subclass_file_path'], 'documentation')
        ))

    def test_file_path_and_pack_path_must_match(self):
        dummy_attrs = {
            'object_type': 'dummy',
            'subclass_file_path': os.path.join('auto_doc', 'dummy'),
            'subclass_pack_path': 'not_a_test',
        }

        with self.assertRaises(AssertionError):
            self.tracked_properties('DummyClass', (), dummy_attrs)


class TrackedColumnTestCase(CommonSetUpTestCase):

    def test_init(self):
        initable_column_class = self.tracked_column.create_dummy_subclass('dummy_column_name')
        test = initable_column_class(self.test_docs)

        self.assertTrue('home_table' in test.documentation)
        self.assertTrue('column_data' in test.documentation)

    def test_create_dummy_instance(self):
        dummy_subclass = self.tracked_column.create_dummy_instance('test_column', self.test_docs)
        self.assertNotEqual(type(dummy_subclass), self.tracked_column)
        self.assertTrue(issubclass(type(dummy_subclass), self.tracked_column))
        self.assertEqual(dummy_subclass.name, 'test_column')
        for key, value in self.test_docs.items():
            self.assertEqual(dummy_subclass.documentation[key], value)

    def test_get_all(self):
        columns = self.tracked_column.get_all()

        self.assertEqual(len(columns), 42)
        self.assertTrue(all(
            column_name in columns
            for column_name in self.column_names
        ))

        for column in columns.values():
            self.assertTrue(isinstance(column, self.tracked_column))

    def test_most_common_vals_to_set(self):
        all_objects = self.tracked_object.get_all_tracked_objects()

        connection = psycopg2.connect(host=self.tracked_object.host, database=self.tracked_object.db)
        cursor = connection.cursor()

        cursor.execute("SELECT most_common_vals FROM pg_stats")
        all_vals = cursor.fetchall()

        connection.close()

        for vals, in all_vals:
            test_set = self.tracked_column.most_common_vals_to_set(vals)
            self.assertTrue(isinstance(test_set, set))
            for val in test_set:
                self.assertTrue(isinstance(val, str))

    def test_possible_values(self):
        tables = self.tracked_table.get_all()

        for table in tables.values():
            cursor = table.get_cursor()
            table.update_snapshot(cursor=cursor, query_mode='FORCE')
            for column in table.columns.values():
                values_from_snapshot = column._db_snapshot.get('column_stats', {}).get('most_common_vals', [])
                values_from_docs = column.documentation['column_stats'].get('most_common_vals', [])
                values_from_decode = set(column.documentation['decode_values'].keys())

                for value in values_from_snapshot:
                    self.assertTrue(value in column.possible_values)

                for value in values_from_docs:
                    self.assertTrue(value in column.possible_values)

                for value in values_from_decode:
                    self.assertTrue(value in column.possible_values)

                for value in column.possible_values:
                    self.assertTrue(
                        value in values_from_snapshot or
                        value in values_from_docs or
                        value in values_from_decode
                    )

    def test_name_fits_convention(self):
        columns = self.tracked_column.get_all()

        self.assertFalse(columns['column_1'].name_fits_convention())
        self.assertTrue(columns['test_table_1_column_1'].name_fits_convention())
        self.assertTrue(columns['test_table_2_column_1'].name_fits_convention())
        self.assertTrue(columns['test_table_4_column_1'].name_fits_convention())

    def test_update_snapshot__stats_changed_coded_column(self):
        columns = self.tracked_column.get_all()

        for column in columns.values():
            if column.is_coded is not False:
                new_column_stats = deepcopy(column.documentation['column_stats'])
                x = 1000
                while x in column.possible_values:
                    x += 1

                if new_column_stats['most_common_vals'] is None:
                    import pdb; pdb.set_trace()
                new_most_common_vals = set(new_column_stats['most_common_vals'])
                new_most_common_vals.add(x)
                new_column_stats['most_common_vals'] = '{{{vals}}}'.format(vals=str.join(',', str(new_most_common_vals)))

                column.update_snapshot(
                    column_data=column.documentation['column_data'],
                    column_stats=new_column_stats,
                )

                self.assertFalse(column.column_changed)
                self.assertTrue(column.values_changed)
                self.assertFalse(column.changed)

    def test_update_snapshot__stats_changed_non_coded_column(self):
        columns = self.tracked_column.get_all()

        for column in columns.values():
            if column.exists:
                new_column_stats = copy(column.documentation['column_stats'])
                if new_column_stats['n_distinct'] != -1:
                    new_column_stats['n_distinct'] = -1
                else:
                    new_column_stats['n_distinct'] = -0.5

                column.update_snapshot(
                    column_data=column.documentation['column_data'],
                    column_stats=new_column_stats,
                )

                self.assertFalse(column.column_changed)
                self.assertFalse(column.values_changed)
                self.assertFalse(column.changed)


    def test_update_snapshot__column_changed(self):
        columns = self.tracked_column.get_all()

        for column in columns.values():
            if column.exists:
                new_column_data = copy(column.documentation['column_data'])
                if new_column_data['ordinal_position'] != 1:
                    new_column_data['ordinal_position'] = 1
                else:
                    new_column_data['ordinal_position'] = 2

                column.update_snapshot(
                    column_data=new_column_data,
                    column_stats=column.documentation['column_stats'],
                )

                self.assertTrue(column.column_changed)
                self.assertFalse(column.values_changed)
                self.assertTrue(column.changed)

    def test_update_snapshot__column_unchanged(self):
        columns = self.tracked_column.get_all()

        for column in columns.values():
            
            column.update_snapshot(
                column_data=column.documentation['column_data'],
                column_stats=column.documentation['column_stats'],
            )

            self.assertFalse(column.column_changed)
            self.assertFalse(column.values_changed) # Even if column is coded,
            # values_changed should only be reported if new values are 
            # discovered, not if old ones are removed since that can happen 
            # by chance due to possible values being an estimate.
            self.assertFalse(column.changed)        

    def test_update_snapshot__column_dropped(self):
        columns = self.tracked_column.get_all()

        for column in columns.values():
            if column.documentation['column_data'] and column.documentation['column_stats']:
                column.update_snapshot(
                    column_data=[],
                    column_stats=[],
                )

                self.assertTrue(column.column_changed)
                self.assertFalse(column.values_changed) # Even if column is coded,
                # values_changed should only be reported if new values are 
                # discovered, not if old ones are removed since that can happen 
                # by chance due to possible values being an estimate.
                self.assertTrue(column.changed)

    def test_update_snapshot__traveling_changed(self):
        columns = self.tracked_column.get_all()

        for column in columns.values():
            if column.exists:
                column.current_table = 'not_' + column.home_table
                new_column_data = copy(column.documentation['column_data'])
                if new_column_data['ordinal_position'] != 1:
                    new_column_data['ordinal_position'] = 1
                else:
                    new_column_data['ordinal_position'] = 2

                column.update_snapshot(
                    column_data=new_column_data,
                    column_stats=column.documentation['column_stats'],
                )

                self.assertFalse(column.column_changed)
                self.assertFalse(column.values_changed)
                self.assertFalse(column.changed)

    def test_update_snapshot__traveling_dropped(self):
        columns = self.tracked_column.get_all()

        for column in columns.values():
            if column.exists:
                column.current_table = 'not_' + column.home_table
                column.update_snapshot(
                    column_data=[],
                    column_stats=[],
                )

                self.assertTrue(column.column_changed)
                self.assertTrue(column.values_changed)
                self.assertTrue(column.changed)

    def test_update_documentation_file__traveling(self):
        columns = self.tracked_column.get_all()

        for column in columns.values():
            if column.exists:
                column.current_table = 'not_' + column.home_table

                column.documentation = {}
                unaltered_docs = json.load(column.doc_file_name)

                column.update_documentation_file()

                self.assertEqual(unaltered_docs, json.load(column.doc_file_name))


class TrackedTableTestCase(CommonSetUpTestCase):

    def test_build_code_template(self):
        test_table_name = 'test_table'
        code_template = self.tracked_table.build_code_template(test_table_name)
        self.assertTrue(isinstance(code_template, str))
        self.assertNotEqual(code_template.find(self.tracked_table.__name__), -1)
        self.assertNotEqual(code_template.find("name = '{}'".format(test_table_name)), -1)

    def test_init(self):

        from .tables.test_table_1 import TestTable1

        test_tab = TestTable1(self.test_docs)

        self.assertTrue('column_data' in test_tab.documentation)
        self.assertTrue('table_data' in test_tab.documentation)
        self.assertTrue('table_bytes' in test_tab.documentation)
        self.assertTrue('renamed_columns' in test_tab.documentation)
        self.assertTrue(hasattr(test_tab, '_columns'))

    def test_columns(self):
        tables = self.tracked_table.get_all()

        connection = psycopg2.connect(host=self.tracked_object.host, database=self.tracked_object.db)
        cursor = connection.cursor()

        cursor.execute("SET search_path TO {schema}".format(schema=self.tracked_object.schema))
        cursor.execute('SET ROLE "{common_user}"'.format(common_user=self.tracked_object.common_user))

        cursor.execute(
            "ALTER TABLE test_table_1 "
            "ADD COLUMN test_table_1_column_3 FLOAT"
        )

        cursor.close()
        connection.commit()

        # column_1 is in the documentation and the database
        # column_2 is only in the documentation
        # and column_3 is only in the database
        # self.tracked_table.columns should pick up all 3
        tables['test_table_1'].update_snapshot()
        tables['test_table_1'].close_connection()
        self.assertTrue('test_table_1_column_1' in tables['test_table_1'].columns)
        self.assertTrue('test_table_1_column_2' in tables['test_table_1'].columns)
        self.assertTrue('test_table_1_column_3' in tables['test_table_1'].columns)
        self.assertEqual(len(tables['test_table_1'].columns), 3)

    def test_columns__traveling_columns(self):
        tables = self.tracked_table.get_all()

        for table in tables.values():
            for column in table.columns.values():
                if column.home_table != table.name:
                    self.assertTrue(column.is_traveling)
                else:
                    self.assertFalse(column.is_traveling)

    def test_change_column_name_in_database(self):
        tables = self.tracked_table.get_all()
        tables['test_table_3'].change_column_name_in_database(
            'column_1',
            'test_table_3_column_1',
        )
        tables['test_table_3'].finalize()

        connection = psycopg2.connect(host=self.tracked_object.host, database=self.tracked_object.db)
        cursor = connection.cursor()

        cursor.execute("SET search_path TO {schema}".format(schema=self.tracked_object.schema))
        cursor.execute('SET ROLE "{common_user}"'.format(common_user=self.tracked_object.common_user))

        cursor.execute(
            "SELECT column_name "
            "FROM information_schema.columns "
            "WHERE table_name = 'test_table_3'"
        )
        column_names = cursor.fetchall()[0]

        cursor.close()
        connection.close()

        self.assertFalse('column_1' in column_names)
        self.assertTrue('test_table_3_column_1' in column_names)

    def test_rename_column__new_column_empty(self):
        tables = self.tracked_table.get_all()
        
        tables['test_table_5'].columns['test_table_5_column_2'].documentation.pop('description')
        column_2_original_docs = tables['test_table_5'].columns['test_table_5_column_2'].documentation

        tables['test_table_5'].rename_column(
            'test_table_5_column_1',
            'test_table_5_column_2',
        )

        self.assertEqual(
            tables['test_table_5'].documentation['renamed_columns']['test_table_5_column_1'],
            'test_table_5_column_2',
        )
        self.assertNotEqual(
            tables['test_table_5'].columns['test_table_5_column_2'].documentation,
            column_2_original_docs,
        )
        self.assertEqual(
            tables['test_table_5'].columns['test_table_5_column_2'].documentation['description'],
            tables['test_table_5'].columns['test_table_5_column_1'].documentation['description'],
        )

    def test_rename_column__neither_column_empty(self):
        tables = self.tracked_table.get_all()

        column_2_original_docs = tables['test_table_5'].columns['test_table_5_column_2'].documentation

        tables['test_table_5'].rename_column(
            'test_table_5_column_1',
            'test_table_5_column_2',
        )

        self.assertEqual(
            tables['test_table_5'].documentation['renamed_columns']['test_table_5_column_1'],
            'test_table_5_column_2',
        )
        self.assertEqual(
            tables['test_table_5'].columns['test_table_5_column_2'].documentation,
            column_2_original_docs,
        )

    def test_rename_column__does_not_change_other_columns(self):
        tables = self.tracked_table.get_all()

        tables['test_table_5'].columns['test_table_5_column_2'].documentation.pop('description')
        column_2_original_docs = tables['test_table_5'].columns['test_table_5_column_2'].documentation

        tables['test_table_5'].rename_column(
            'test_table_5_column_1',
            'test_table_5_arbitrary_new_column_name_1',
        )

        self.assertEqual(len(tables['test_table_5'].columns), 4)
        self.assertEqual(
            tables['test_table_5'].documentation['renamed_columns']['test_table_5_column_1'],
            'test_table_5_arbitrary_new_column_name_1',
        )
        self.assertTrue(all(
            key == column.name
            for key, column in tables['test_table_5'].columns.items()
        ))

    def test_update_snapshot__stats_changed_coded_column(self):
        tables = self.tracked_table.get_all()

        for table in tables.values():
            cursor = table.get_cursor()
            table.update_snapshot(cursor=cursor, query_mode='FORCE')

            previous_coded_states = {}
            for column in table.columns.values():
                if column.exists and not column.is_traveling and column.documentation['column_data']['data_type'] == 'NUMERIC':
                    previous_coded_states[column.name] = column.is_coded
                    if column.is_coded is not False:
                        cursor.execute(
                            "INSERT INTO {table} ({column}) "
                            "SELECT t.i "
                            "FROM generate_series(1, 100) as t(i), generate_series(1, 15)".format(
                                table=table.name,
                                column=column.name,
                            )
                        )
                    else:
                        cursor.execute(
                            "UPDATE {table} SET {column} = 1".format(
                                table=table.name,
                                column=column.name,
                            )
                        )

            table.update_snapshot(cursor=cursor, query_mode='FORCE')
            for column in table.columns.values():
                if column.exists and not column.is_traveling and column.documentation['column_data']['data_type'] == 'NUMERIC':
                    if previous_coded_states[column.name] is True:
                        self.assertTrue(column.is_coded)
                        self.assertTrue(column.values_changed)
                        self.assertTrue(column.changed)
                        self.assertTrue(len(column.possible_values) > self.coded_column_cap)
                    elif previous_coded_states[column.name] is None:
                        self.assertFalse(column.is_coded)
                        self.assertFalse(column.values_changed)
                        self.assertFalse(column.changed)
                    elif previous_coded_states[column.name] is False:
                        self.assertTrue(column.is_coded is None)
                        self.assertTrue(column.values_changed)
                        self.assertTrue(column.changed)
        table.close_connection()

    def test_update_snapshot__exists_traveling(self):
        tables = self.tracked_table.get_all()

        for table in tables.values():
            cursor = table.get_cursor()
            table.update_snapshot(cursor=cursor, query_mode='FORCE')

            for column in table.columns.values():
                if column.exists and column.is_traveling:
                    # Change column
                    if column.documentation['column_data'].get('data_type', '').upper() != 'TEXT':
                        cursor.execute(
                            "ALTER TABLE {table} "
                            "ALTER COLUMN {column} TYPE TEXT".format(
                                table=table.name,
                                column=column.name,
                            )
                        )
                    else:
                        cursor.execute(
                            "ALTER TABLE {table} "
                            "ALTER COLUMN {column} TYPE VARCHAR(50)".format(
                                table=table.name,
                                column=column.name,
                            )
                        )

                    # Change Values
                    if column.is_coded is not False:
                        cursor.execute(
                            "INSERT INTO {table} ({column}) "
                            "SELECT t.i "
                            "FROM generate_series(1, 100) as t(i), generate_series(1, 15)".format(
                                table=table.name,
                                column=column.name,
                            )
                        )
                    else:
                        cursor.execute(
                            "UPDATE {table} SET {column} = 1".format(
                                table=table.name,
                                column=column.name,
                            )
                        )

            table.update_snapshot(cursor=cursor, query_mode='FORCE')
            for column in table.columns.values():
                if column.exists and column.is_traveling:
                    self.assertFalse(column.values_changed)
                    self.assertFalse(column.column_changed)
                    self.assertFalse(column.changed)
            table.close_connection()

    def test_update_snapshot__does_not_exist_traveling(self):
        tables = self.tracked_table.get_all()

        for table in tables.values():
            cursor = table.get_cursor()
            table.update_snapshot(cursor=cursor, query_mode='FORCE')

            previously_extant_columns = copy(table.extant_columns)
            cursor.execute("DROP TABLE {table}".format(table=table.name))

            table.update_snapshot(cursor=cursor, query_mode='FORCE')
            for column in table.columns.values():
                if column.is_traveling:
                    self.assertFalse(column.exists)
                    self.assertEqual(column.changed, column.name in previously_extant_columns)
            table.close_connection()

    def test_update_snapshot__column_data_changed(self):
        tables = self.tracked_table.get_all()

        for table in tables.values():
            cursor = table.get_cursor()
            table.update_snapshot(cursor=cursor, query_mode='FORCE')

            for column in table.columns.values():
                if column.exists and not column.is_traveling:
                    # Change column
                    if column.documentation['column_data']['data_type'].upper() != 'TEXT':
                        cursor.execute(
                            "ALTER TABLE {table} "
                            "ALTER COLUMN {column} TYPE TEXT".format(
                                table=table.name,
                                column=column.name,
                            )
                        )
                    else:
                        cursor.execute(
                            "ALTER TABLE {table} "
                            "ALTER COLUMN {column} TYPE VARCHAR(50)".format(
                                table=table.name,
                                column=column.name,
                            )
                        )

            table.update_snapshot(cursor=cursor, query_mode='FORCE')
            for column in table.columns.values():
                if column.exists and not column.is_traveling:
                    self.assertTrue(column.column_changed)
                    self.assertTrue(column.changed)
            table.close_connection()

    def test_update_snapshot__column_data_same(self):
        tables = self.tracked_table.get_all()

        for table in tables.values():
            cursor = table.get_cursor()
            table.update_snapshot(cursor=cursor, query_mode='FORCE')
            for column in table.columns.values():
                cursor.execute("SELECT column_name FROM information_schema.columns WHERE table_name = '{table}'".format(table=table.name))
                db_column_names = [column_name for column_name, in cursor.fetchall()]
                self.assertEqual(column.name in db_column_names, column.exists)
                column_has_docs = bool(column.documentation['column_data']) or column.is_traveling
                if column_has_docs:
                    if column.exists:
                        self.assertFalse(column.changed)
                    else:
                        self.assertTrue(column.changed)
                else:
                    if column.exists:
                        self.assertTrue(column.changed)
                    else:
                        self.assertFalse(column.changed)
            table.close_connection()

    def test_update_snapshot__column_data_gone(self):
        tables = self.tracked_table.get_all()

        for table in tables.values():
            cursor = table.get_cursor()
            table.update_snapshot(cursor=cursor, query_mode='FORCE')

            cursor.execute("DROP TABLE {table}".format(table=table.name))

            table.update_snapshot(cursor=cursor, query_mode='FORCE')
            for column in table.columns.values():
                if not column.is_traveling:
                    column_has_docs = bool(column.documentation['column_data'])
                    self.assertFalse(column.exists)
                    self.assertEqual(column.column_changed, column_has_docs)
                    self.assertEqual(column.changed, column_has_docs)
            table.close_connection()

    def test_finalize__grants_privileges(self):
        tables = self.tracked_table.get_all()

        for table in tables.values():
            table.tear_down()
            table.set_up()
            table.finalize()

            cursor = table.get_cursor()
            cursor.execute(
                "SELECT * FROM information_schema.table_privileges "
                "WHERE table_name = '{table}'".format(
                    table=table.name,
                )
            )
            privileges = cursor.fetchall()
            privileges = [
                {
                    'grantor': grantor,
                    'grantee': grantee,
                    'table_catalog': table_catalog,
                    'table_schema': table_schema,
                    'table_name': table_name,
                    'privilege_type': privilege_type, 
                    'is_grantable': is_grantable,
                    'with_hierarchy': with_hierarchy,
                }
                for (
                    grantor,
                    grantee,
                    table_catalog,
                    table_schema,
                    table_name,
                    privilege_type, 
                    is_grantable,
                    with_hierarchy,
                ) in privileges
            ]

            public_privileges = [
                privilege
                for privilege in privileges
                if privilege['grantee'] == 'PUBLIC'
            ]
            privilege_types = {
                'SELECT',
                'INSERT', 
                'UPDATE',
                'DELETE',
                'TRUNCATE',
                'REFERENCES',
                'TRIGGER',
            }
            self.assertEqual(len(public_privileges), 7)
            self.assertEqual(
                privilege_types,
                {
                    privilege['privilege_type']
                    for privilege in public_privileges
                }
            )
            table.close_connection()

    def test_finalize__creates_index(self):
        tables = self.tracked_table.get_all()

        for table in tables.values():
            table.tear_down()
            table.set_up()

            cursor = table.get_cursor()
            cursor.execute(
                "SELECT * FROM pg_indexes "
                "WHERE tablename = '{table}'".format(
                    table=table.name,
                )
            )
            self.assertEqual(len(cursor.fetchall()), 0)
            table.finalize()

            cursor = table.get_cursor()
            cursor.execute(
                "SELECT * FROM pg_indexes "
                "WHERE tablename = '{table}'".format(
                    table=table.name,
                )
            )
            indexes = cursor.fetchall()
            indexes = [
                {
                    'schemaname': schemaname,
                    'tablename': tablename,
                    'indexname': indexname,
                    'tablespace': tablespace,
                    'indexdef': indexdef,
                }
                for (
                    schemaname,
                    tablename,
                    indexname,
                    tablespace,
                    indexdef,
                ) in indexes
            ]
            for index in indexes:
                self.assertTrue(index['indexname'].startswith(table.name))
                self.assertTrue(index['indexname'][len(table.name) + 1:-4] in table.extant_columns.keys())

            table.close_connection()

    def test_finalize__does_not_create_duplicate_indexes(self):
        tables = self.tracked_table.get_all()

        for table in tables.values():
            # Delete table to ensure that it has no existing indexes.
            table.tear_down()
            table.set_up()

            cursor = table.get_cursor()
            cursor.execute(
                "SELECT * FROM pg_indexes "
                "WHERE tablename = '{table}'".format(
                    table=table.name,
                )
            )
            self.assertEqual(len(cursor.fetchall()), 0)
            table.finalize()

            cursor = table.get_cursor()
            cursor.execute(
                "SELECT * FROM pg_indexes "
                "WHERE tablename = '{table}'".format(
                    table=table.name,
                )
            )
            indexes = cursor.fetchall()

            table.update_snapshot()
            extant_columns = [column.name for column in table.columns.values() if column.exists]
            indexed_columns = set(extant_columns).intersection(set(self.tracked_table.auto_index_columns))

            self.assertEqual(len(indexed_columns), len(indexes))
            table.finalize()

            cursor = table.get_cursor()
            cursor.execute(
                "SELECT * FROM pg_indexes "
                "WHERE tablename = '{table}'".format(
                    table=table.name,
                )
            )
            indexes = cursor.fetchall()

            table.update_snapshot()
            extant_columns = [column for column in table.columns.values() if column.exists]
            self.assertEqual(len(indexed_columns), len(indexes))
            table.close_connection()

    def test_finalize__renames_columns(self):
        tables = self.tracked_table.get_all()

        for table in tables.values():
            table.update_snapshot()

            for i, column in enumerate(table.columns):
                table.rename_column(column, 'some_random_column_name_' + str(i))
            table.finalize()

            self.assertTrue(all([
                column_name.startswith('some_random_column_name_')
                for column_name in table.extant_columns.keys()
            ]))
            table.close_connection()

    def test_finalize__leaves_documentation_intact(self):
        tables = self.tracked_table.get_all()

        for table in tables.values():
            table.update_snapshot()

            for column in table.columns.values():
                column.home_table = None

            table.tear_down()
            table.set_up()
            table.finalize()

        for table in tables.values():
            self.assertTrue(os.path.exists(table.doc_file_name))

    def test_update_documentation_file__updates_table_documentation(self):
        tables = self.tracked_table.get_all()

        tables['test_table_1'].documentation['test_attr'] = 'test_attr_value'
        tables['test_table_1'].documentation['dependencies'] = [
            'test_table_2',
        ]

        tables['test_table_1'].update_documentation_file()

        self.assertTrue(os.path.exists(tables['test_table_1'].doc_file_name))
        self.assertTrue(os.path.exists(tables['test_table_2'].doc_file_name))

        with open(tables['test_table_1'].doc_file_name, 'r') as table_1_doc_file:
            table_1_docs = json.load(table_1_doc_file)
        self.assertTrue('test_attr' in table_1_docs['test_table_1'])
        self.assertTrue('dependencies' in table_1_docs['test_table_1'])
        self.assertTrue(
            'test_table_2' in
            table_1_docs['test_table_1']['dependencies']
        )

        with open(tables['test_table_2'].doc_file_name, 'r') as table_2_doc_file:
            table_2_docs = json.load(table_2_doc_file)
        self.assertTrue('descendents' in table_2_docs['test_table_2'])
        self.assertTrue(
            'test_table_1' in
            table_2_docs['test_table_2']['descendents']
        )

    def test_update_documentation_file__captures_renaming(self):
        tables = self.tracked_table.get_all()

        tables['test_table_1'].rename_column('test_table_1_column_1', 'some_arbitrary_column_name')

        tables['test_table_1'].update_documentation_file()

        self.assertTrue(os.path.exists(tables['test_table_1'].doc_file_name))

        with open(tables['test_table_1'].doc_file_name, 'r') as table_1_doc_file:
            table_1_docs = json.load(table_1_doc_file)
        self.assertTrue('renamed_columns' in table_1_docs['test_table_1'])
        self.assertEqual(
            table_1_docs['test_table_1']['renamed_columns']['test_table_1_column_1'],
            'some_arbitrary_column_name',
        )

    def test_update_documentation_file__updates_columns(self):
        tables = self.tracked_table.get_all()
        table = tables['test_table_1']
        table_4_column_1 = tables['test_table_4'].columns['test_table_4_column_1']
        columns = table.columns

        columns['test_table_1_column_1'].documentation['test_attr'] = 'test_attr_value'
        columns['test_table_1_column_1'].documentation['dependencies'] = [
            'test_table_4_column_1',
        ]

        table.update_snapshot()
        table.update_documentation_file()
        

        # No files should be deleted, even for columns that no longer exist
        self.assertTrue(os.path.exists(columns['test_table_1_column_1'].doc_file_name))
        self.assertTrue(os.path.exists(columns['test_table_1_column_2'].doc_file_name))
        self.assertTrue(os.path.exists(table_4_column_1.doc_file_name))

        with open(columns['test_table_1_column_1'].doc_file_name, 'r') as column_1_doc_file:
            column_1_docs = json.load(column_1_doc_file)
        self.assertTrue('test_attr' in column_1_docs['test_table_1_column_1'])
        self.assertTrue('dependencies' in column_1_docs['test_table_1_column_1'])
        self.assertTrue(
            'test_table_4_column_1' in
            column_1_docs['test_table_1_column_1']['dependencies']
        )

        with open(table_4_column_1.doc_file_name, 'r') as column_4_doc_file:
            column_4_docs = json.load(column_4_doc_file)
        self.assertTrue('descendents' in column_4_docs['test_table_4_column_1'])
        self.assertTrue(
            'test_table_1_column_1' in
            column_4_docs['test_table_4_column_1']['descendents']
        )

    def test_update_documentation_file__updates_column_home_tables(self):
        tables = self.tracked_table.get_all()

        for table in tables.values():
            table.update_snapshot()

            for column in table.columns.values():
                column.home_table = None
            table.update_documentation_file()

            for column in table.columns.values():
                self.assertNotEqual(column.home_table, None)
                if not column.name.startswith('column_1'):
                    self.assertTrue(column.name.startswith(column.home_table))

    def test_update_documentation_file__does_not_delete_columns(self):
        tabs = self.tracked_table.get_all()

        test_table_4 = tabs['test_table_4']
        test_table_2 = tabs['test_table_2']

        self.assertFalse(test_table_2.is_updated)
        self.assertTrue('test_table_2' in test_table_4.documentation['dependencies'])

        test_table_4.update_documentation_file()

        for column in test_table_4.columns.values():
            self.assertTrue(os.path.exists(column.doc_file_name))

        for column in test_table_2.columns.values():
            self.assertTrue(os.path.exists(column.doc_file_name))

    def test_update_documentation_file__does_not_update_non_home_table(self):
        tabs = self.tracked_table.get_all()

        for table in tabs.values():
            table.update_snapshot()
            if any([col.home_table != table.name for col in table.columns.values()]):
                cursor = table.get_cursor()

                column_docs = {}
                for column in table._db_snapshot['column_data'].values():
                    # Change column
                    if column.get('data_type', '').upper() != 'TEXT':
                        cursor.execute(
                            "ALTER TABLE {table} "
                            "ALTER COLUMN {column} TYPE TEXT".format(
                                table=table.name,
                                column=column['column_name'],
                            )
                        )
                    else:
                        cursor.execute(
                            "ALTER TABLE {table} "
                            "ALTER COLUMN {column} TYPE VARCHAR(50)".format(
                                table=table.name,
                                column=column['column_name'],
                            )
                        )
                    column_docs[column['column_name']] = copy(table.columns[column['column_name']].documentation)
                table._connection.commit()

                table.update_snapshot(query_mode='FORCE')
                table.update_documentation_file()

                for column in table.extant_columns.values():
                    if column.home_table == table.name:
                        self.assertNotEqual(column_docs[column.name], column.documentation)
                    else:
                        self.assertEqual(column_docs[column.name], column.documentation)

    def test_update_documentation_file__updates_descendents_in_dependency_files(self):
        tabs = self.tracked_table.get_all()
        all_objects = self.tracked_object.get_all_tracked_objects()

        for tab in tabs.values():
            if tab.documentation['dependencies']:
                for dependency_name in tab.documentation['dependencies']:
                    dependency = all_objects[dependency_name]
                    dependency.documentation['descendents'] = []
                    os.remove(dependency.doc_file_name)
                    dependency.update_documentation_file(insert_self_into_descendents=False)

                    with open(dependency.doc_file_name) as dependency_docs:
                        self.assertEqual(json.load(dependency_docs)[dependency.name]['descendents'], [])

                tab.update_documentation_file()

                for dependency_name in tab.documentation['dependencies']:
                    dependency = all_objects[dependency_name]
                    with open(dependency.doc_file_name) as dependency_docs:
                        self.assertEqual(json.load(dependency_docs)[dependency.name]['descendents'], [tab.name])

class RunTestCase(CommonSetUpTestCase):

    def test_document_description__saves_documentation(self):
        tables = self.tracked_table.get_all()
        test_description = 'test_description'

        for table in tables.values():
            with mock.patch('sys.stdout', new=StringIO()), mock.patch('auto_doc.run.get_input_from_editor'), mock.patch('auto_doc.run.read_from_editor', side_effect=[test_description]):
                document_description(table)
                self.assertEqual(table.documentation['description'], test_description)
                with open(table.doc_file_name) as doc_file:
                    self.assertEqual(json.load(doc_file)[table.name]['description'], test_description)

    def test_rename_colums__saves_documentation(self):
        tables = self.tracked_table.get_all()

        for table in tables.values():
            table.update_snapshot()
            non_conforming_columns = []
            for column in table.columns.values():
                if not column.name_fits_convention():
                    non_conforming_columns.append(column.name)
            if not non_conforming_columns:
                continue

            def enter_blank():
                return ''

            with mock.patch('sys.stdout', new=StringIO()), mock.patch('auto_doc.run.get_input', side_effect=enter_blank):
                rename_columns(table.name, tables)
            for column in non_conforming_columns:
                self.assertTrue(column in table.documentation['renamed_columns'])
                with open(table.doc_file_name) as doc_file:
                    self.assertTrue(column in json.load(doc_file)[table.name]['renamed_columns'])

    def test_document_columns__saves_documentation(self):
        tables = self.tracked_table.get_all()
        test_description = 'test_description'

        for table in tables.values():
            side_effect_lists = []
            for i in range(len(table.columns)):
                side_effect_list = []
                for j in range(i):
                    side_effect_list.append(test_description)
                side_effect_list.append('')
                side_effect_lists.append(side_effect_list)

            for side_effect_list, column in zip(
                side_effect_lists,
                sorted(
                    table.extant_columns.values(),
                    key=lambda x: x.name,
                ),
            ):
                with (
                    mock.patch('sys.stdout', new=StringIO()),
                    mock.patch('run.get_input_from_editor'),
                    mock.patch('run.read_from_editor', side_effect=side_effect_list),
                ):
                    document_columns(table)
                    self.assertEqual(table.column[column.name].documentation['description'], test_description)
                    with open(table.column[column.name].doc_file_name) as doc_file:
                        self.assertEqual(json.load(doc_file)[column.name]['description'], test_description)

    def test_document_possible_values__saves_extra_values_to_documentation(self):
        tables = self.tracked_table.get_all()
        test_description = 'test_description'

        for table in tables.values():
            side_effect_list = ['YES', 'new_value', 'YES', 'second_one', 'YES']
            for column in sorted(table.extant_columns.values(), key=lambda x: x.name):
                if column.is_coded is not False:
                    first_coded_column = column.name
                    break
            else:
                continue

            with (
                mock.patch('sys.stdout', new=StringIO()),
                mock.patch('run.get_input', side_effect=side_effect_list),
                mock.patch('run.get_input_from_editor'),
                mock.patch('run.read_from_editor', side_effect=NotImplementedError),
            ):
                with self.assertRaises(NotImplementedError):
                    document_possible_values(table)
     
            self.assertTrue('new_value' in table.columns[first_coded_column].documentation['decode_values'])
            self.assertTrue('second_one' in table.columns[first_coded_column].documentation['decode_values'])
            with open(table.columns[first_coded_column].doc_file_name) as doc_file:
                self.assertTrue('new_value' in json.load(doc_file)[first_coded_column]['decode_values'])
                self.assertTrue('second_one' in json.load(doc_file)[first_coded_column]['decode_values'])

    def test_document_possible_values__saves_descriptions_to_documentation(self):
        tables = self.tracked_table.get_all()
        test_description = 'test_description'

        for table in tables.values():
            side_effect_list = []
            base_side_effect_list = ['YES', 'NO']
            coded_columns = []
            for column in sorted(table.extant_columns.values(), key=lambda x: x.name):
                if column.is_coded is not False:
                    codded_columns.append(column.name)
                    side_effect_list.append(copy(base_side_effect_list))
            if not coded_columns:
                continue

            with (
                mock.patch('sys.stdout', new=StringIO()),
                mock.patch('run.get_input', side_effect=side_effect_list),
                mock.patch('run.get_input_from_editor'),
                mock.patch('run.read_from_editor', side_effect=test_description),
            ):
                document_possible_values(table)

            for column in coded_columns:
                self.assertTrue(all(
                    value == test_description
                    for value in table.columns[column].documentation['decode_values'].values()
                ))
                with open(table.columns[first_coded_column].doc_file_name) as doc_file:
                    self.assertTrue(all(
                        value == test_description
                        for value in json.load(doc_file)[first_coded_column]['decode_values'].values()
                    ))

    def test_get_input_from_editor(self):
        with open(TEMP_INPUT_PATH, 'w') as f:
            f.write('this should be overwritten')

        with mock.patch('subprocess.call') as s_call:
            get_input_from_editor('# some prompt\n', 'initial response')
            s_call.assert_called_with([TEMP_INPUT_PATH], shell=True)

        with open(TEMP_INPUT_PATH, 'r') as f:
            self.assertEqual(f.read(), '# some prompt\ninitial response')

    def test_read_from_editor(self):
        with open(TEMP_INPUT_PATH, 'w') as f:
            f.write('this should be picked up')
            f.write("\n# this shouldn't")
            f.write("\n               # neither should this")
            f.write("\n but all of #this should")

        self.assertEqual(read_from_editor(), 'this should be picked up\n but all of #this should')

    def test_read_from_editor__sanitizes_input(self):
        shutil.copyfile(
            os.path.join(
                os.path.dirname(__file__),
                'initial_docs',
                'misc',
                'broken_user_input.txt',
            ),
            TEMP_INPUT_PATH
        )

        # broken_user_input contains invalid unicode
        test = read_from_editor()

        # Once we save our broken unicode to a .json file
        # it will cause an error if it hasn't been sanitized
        tables = self.tracked_object.get_all_tracked_objects()
        table_1 = tables['test_table_1']
        table_1.documentation['description'] = test
        table_1.update_documentation_file()

        with open(table_1.doc_file_name, encoding='latin-1', errors='backslashescape') as doc_file:
            json.load(doc_file)

    def test_order_tables_by_dependencies__correct_order(self):
        tables = self.tracked_object.get_all_tracked_objects()

        table_names = list(tables.keys())

        ordered_tables = order_tables_by_dependencies(table_names, tables)

        # Assert none of test_table_1's descendents come before it
        self.assertNotIn('test_table_2', ordered_tables[0:ordered_tables.index('test_table_1')])
        self.assertNotIn('test_table_4', ordered_tables[0:ordered_tables.index('test_table_1')])
        self.assertNotIn('test_object_4', ordered_tables[0:ordered_tables.index('test_table_1')])

        # Assert none of test_table_2's descendents come before it
        self.assertNotIn('test_table_4', ordered_tables[0:ordered_tables.index('test_table_2')])

    def test_order_tables_by_dependencies__error_msg(self):

        tables = self.tracked_table.get_all()
        table_names = list(tables.keys())
        table_names.remove('test_table_1')

        tables['test_table_1'].tear_down()
        tables['test_table_1'].finalize()

        with self.assertRaises(DependecyError) as context:
            order_tables_by_dependencies(table_names, tables)

        msg = context.exception.args[0]
        msg = msg.split('\n')[1:]

        msg_dict = {}
        for text in msg:
            if not text:
                continue
            elif not text.startswith(' '):
                cur_top_key = text
                msg_dict[cur_top_key] = {}
            elif text.startswith(' '*8):
                key, value = text.lstrip(' ').split(': ')
                msg_dict[cur_top_key][cur_bot_key][key] = value
            elif text.startswith(' '*4):
                cur_bot_key = text.lstrip(' ')
                msg_dict[cur_top_key][cur_bot_key] = {}

        table_attrs = {
            "exists",
            "ready",
        }

        self.assertEqual(set(msg_dict.keys()), {'test_table_4', 'test_table_2'})

        self.assertEqual(set(msg_dict['test_table_2'].keys()), {'test_table_1'})
        self.assertEqual(set(msg_dict['test_table_2']['test_table_1']), table_attrs)
        self.assertEqual(msg_dict['test_table_2']['test_table_1']['exists'], 'False')
        self.assertEqual(msg_dict['test_table_2']['test_table_1']['ready'], 'False')

        self.assertEqual(set(msg_dict['test_table_4'].keys()), {'test_table_1'})
        self.assertEqual(set(msg_dict['test_table_4']['test_table_1']), table_attrs)
        self.assertEqual(msg_dict['test_table_4']['test_table_1']['exists'], 'False')
        self.assertEqual(msg_dict['test_table_4']['test_table_1']['ready'], 'False')

    def test_build_tables(self):
        tables = self.tracked_table.get_all()

        connection = psycopg2.connect(host=self.tracked_object.host, database=self.tracked_object.db)
        cursor = connection.cursor()
        cursor.execute("DROP TABLE test_table_1")
        connection.commit()

        # silence print commands
        with mock.patch('sys.stdout', new=StringIO()):
            build_tables(['test_table_1'], tables, force_rebuild=True)

        connection = psycopg2.connect(host=self.tracked_object.host, database=self.tracked_object.db)
        cursor = connection.cursor()
        cursor.execute("SELECT * FROM information_schema.tables WHERE table_name = 'test_table_1'")
        self.assertTrue(bool(cursor.fetchone()))
        connection.close()

    def test_build_tables__rebuilds_with_force(self):
        tables = self.tracked_table.get_all()

        connection = psycopg2.connect(host=self.tracked_object.host, database=self.tracked_object.db)
        cursor = connection.cursor()
        cursor.execute("ALTER TABLE test_table_1 ADD COLUMN should_not_be_here NUMERIC DEFAULT 0")
        connection.commit()

        with mock.patch('sys.stdout', new=StringIO()):
            build_tables(['test_table_1'], tables, force_rebuild=True)

        connection = psycopg2.connect(host=self.tracked_object.host, database=self.tracked_object.db)
        cursor = connection.cursor()
        cursor.execute("SELECT column_name FROM information_schema.columns WHERE table_name = 'test_table_1'")
        test_table_1_columns = [column_name[0] for column_name in cursor.fetchall()]
        connection.close()

        self.assertEqual(test_table_1_columns, list(tables['test_table_1'].extant_columns.keys()))

    def test_document_description(self):
        tables = self.tracked_table.get_all()

        test_description = 'test description'

        for table in tables.values():
            # silence print commands, Supress text editor, Spoof user input to the text editor
            with mock.patch('sys.stdout', new=StringIO()), mock.patch('auto_doc.run.get_input_from_editor'), mock.patch('auto_doc.run.read_from_editor', side_effect=[test_description]):
                document_description(table)

            self.assertEqual(table.description, test_description)

    def test_document_description__rollback_on_blank_description(self):
        tables = self.tracked_table.get_all()

        test_description = 'test description'

        for table in tables.values():
            # silence print commands
            with mock.patch('sys.stdout', new=StringIO()), mock.patch('auto_doc.run.get_input_from_editor'), mock.patch('auto_doc.run.read_from_editor', side_effect=['']):
                self.assertFalse(document_description(table))

            self.assertNotEqual(table.description, test_description)


    def test_document_tables(self):
        tables = self.tracked_table.get_all()

        test_description = 'test description'

        for table in tables.values():
            # silence print commands
            with mock.patch('sys.stdout', new=StringIO()), mock.patch('auto_doc.run.get_input_from_editor'), mock.patch('auto_doc.run.read_from_editor', side_effect=[test_description]):
                document_description(table)

            self.assertEqual(table.description, test_description)

    def test_rename_columns__does_nothing_for_correctly_named_columns(self):
        tables = self.tracked_table.get_all()
        table_1 = tables['test_table_1']
        column_1 = table_1.columns['test_table_1_column_1']

        # silence print commands, Spoof user input to the command line
        with mock.patch('sys.stdout', new=StringIO()), mock.patch('auto_doc.run.get_input', side_effect=['N']):
            for column in table_1.columns.values():
                self.assertTrue(column.name_fits_convention)
            self.assertEqual(table_1.documentation['renamed_columns'], {})

            rename_columns(
                table_1.name,
                tables,
            )

            for column in table_1.columns.values():
                self.assertTrue(column.name_fits_convention)
            self.assertEqual(table_1.documentation['renamed_columns'], {})            

    def test_rename_column__identifies_incorrectly_named_columns_and_has_reasonable_default(self):
        tables = self.tracked_table.get_all()
        table_3 = tables['test_table_3']
        table_3.update_snapshot()
        column_1 = table_3.columns['column_1']

        # silence print commands
        with mock.patch('sys.stdout', new=StringIO()), mock.patch('auto_doc.run.get_input', side_effect=['', '']) as mock_input:
            rename_columns(
                table_3.name,
                tables,
            )

        self.assertTrue(mock_input.called)
        self.assertEqual(list(mock_input.side_effect), [])
        self.assertTrue('test_table_3_column_1' in table_3.columns)
        self.assertEqual(table_3.documentation['renamed_columns']['column_1'], 'test_table_3_column_1')

    def test_rename_column__does_not_force_rename_after_changed_home_table(self):
        tables = self.tracked_table.get_all()
        table_2 = tables['test_table_2']
        table_2.update_snapshot()
        table_2.close_connection()
        column_1 = table_2.columns['test_table_2_column_1']
        column_1.home_table = 'test_table_1'

        # silence print commands, Spoof a series of user inputs
        with mock.patch('sys.stdout', new=StringIO()), mock.patch('auto_doc.run.get_input', side_effect=['test_table_2', 'this_should_be_unused']) as mock_input:
            rename_columns(
                table_2.name,
                tables,
            )

        self.assertTrue(mock_input.called)
        self.assertEqual(list(mock_input.side_effect), ['this_should_be_unused'])

    def test_rename_column__updates_renamed_columns(self):
        tables = self.tracked_table.get_all()
        table_3 = tables['test_table_3']
        table_3.update_snapshot()
        column_1 = table_3.columns['column_1']

        # silence print commands, Spoof a series of user inputs
        with mock.patch('sys.stdout', new=StringIO()), mock.patch('auto_doc.run.get_input', side_effect=['test_table_3', 'test_table_3_column_1']):
            rename_columns(
                table_3.name,
                tables,
            )

        self.assertTrue('column_1' in table_3.documentation['renamed_columns'])
        self.assertEqual(table_3.documentation['renamed_columns']['column_1'], 'test_table_3_column_1')
        self.assertEqual(
            table_3.columns['column_1'].description,
            table_3.columns['test_table_3_column_1'].description,
        )

    def test_rename_column__identifies_deleted_columns(self):
        tables = self.tracked_table.get_all()
        table_5 = tables['test_table_5']

        connection = psycopg2.connect(host=self.tracked_object.host, database=self.tracked_object.db)
        cursor = connection.cursor()

        # Remove two columns that would normally be handled first by rename_columns
        cursor.execute("ALTER TABLE test_table_5 DROP COLUMN test_table_5_column_1")
        cursor.execute("ALTER TABLE test_table_5 DROP COLUMN test_table_3_column_1")

        # Create a renamed column
        cursor.execute("ALTER TABLE test_table_5 DROP COLUMN test_table_5_column_2")
        cursor.execute("ALTER TABLE test_table_5 ADD COLUMN test_table_5_column_3 TEXT DEFAULT ''")

        cursor.close()
        connection.commit()
        table_5.update_snapshot()
        table_5.close_connection()

        column_3 = table_5.columns['test_table_5_column_3']
        # assert that the column we care about is the first one to be handled
        self.assertEqual(
            column_3.name,
            sorted(table_5.extant_columns)[0],
        )

        # silence print commands, Spoof a series of user inputs
        with mock.patch('sys.stdout', new=StringIO()), mock.patch('auto_doc.run.get_input', side_effect=['test_table_5_column_2']):
            rename_columns(
                table_5.name,
                tables,
            )

        self.assertEqual(
            table_5.documentation['renamed_columns']['test_table_5_column_2'],
            'test_table_5_column_3',
        )
        self.assertEqual(
            table_5.columns['test_table_5_column_2'].description,
            table_5.columns['test_table_5_column_3'].description,
        )

    def test_rename_column__identifies_deleted_columns_for_renamed_column(self):
        tables = self.tracked_table.get_all()
        table_5 = tables['test_table_5']

        connection = psycopg2.connect(host=self.tracked_object.host, database=self.tracked_object.db)
        cursor = connection.cursor()

        cursor.execute("ALTER TABLE test_table_5 DROP COLUMN test_table_5_column_2")
        cursor.execute("ALTER TABLE test_table_5 ADD COLUMN column_3 TEXT DEFAULT ''")

        cursor.close()
        connection.commit()
        table_5.update_snapshot()
        table_5.close_connection()

        column_3 = table_5.columns['column_3']
        # assert that the column we care about is the first one to be handled
        self.assertEqual(
            column_3.name,
            sorted(table_5.extant_columns)[0],
        )

        # silence print commands, Spoof a series of user inputs that give
        # appropiate responses to rename 'column_3'
        with mock.patch('sys.stdout', new=StringIO()), mock.patch('auto_doc.run.get_input', side_effect=['test_table_5', 'test_table_5_column_3', 'test_table_5_column_2']):
            # After the first column is handled, mock will run out of input and throw StopIteration
            with self.assertRaises(StopIteration):
                rename_columns(
                    table_5.name,
                    tables,
                )

        self.assertEqual(table_5.documentation['renamed_columns']['test_table_5_column_2'], 'test_table_5_column_3')
        self.assertEqual(
            table_5.columns['test_table_5_column_2'].description,
            table_5.columns['test_table_5_column_3'].description,
        )

    def test_rename_column__understands_no(self):
        tables = self.tracked_table.get_all()
        table_5 = tables['test_table_5']

        connection = psycopg2.connect(host=self.tracked_object.host, database=self.tracked_object.db)
        cursor = connection.cursor()

        cursor.execute("ALTER TABLE test_table_5 DROP COLUMN test_table_5_column_2")
        cursor.execute("ALTER TABLE test_table_5 ADD COLUMN column_3 TEXT DEFAULT ''")

        cursor.close()
        connection.commit()
        table_5.update_snapshot()
        table_5.close_connection()

        column_3 = table_5.columns['column_3']
        # assert that the column we care about is the first one to be handled
        self.assertEqual(
            column_3.name,
            sorted(table_5.extant_columns)[0],
        )

        # silence print commands, Spoof a series of user inputs that give
        # appropiate responses to rename 'column_3'
        with mock.patch('sys.stdout', new=StringIO()), mock.patch('auto_doc.run.get_input', side_effect=['test_table_5', 'test_table_5_column_3', 'N']):
            # After the first column is handled, mock will run out of input and throw StopIteration
            with self.assertRaises(StopIteration):
                rename_columns(
                    table_5.name,
                    tables,
                )

        self.assertFalse('test_table_5_column_2' in table_5.documentation['renamed_columns'])

    def test_document_columns(self):
        tables = self.tracked_table.get_all()
        columns = self.tracked_column.get_all()

        test_description = 'test description'

        for table in tables.values():
            # silence print commands, Spoof a series of user inputs
            with mock.patch('sys.stdout', new=StringIO()), mock.patch('auto_doc.run.get_input', side_effect=['N']), mock.patch('auto_doc.run.get_input_from_editor'), mock.patch('auto_doc.run.read_from_editor', side_effect=[test_description]):
                self.assertTrue(document_columns(table))

            for column in table.extant_columns.values():
                self.assertEqual(column.description, test_description)

    def test_document_columns__rollback_on_blank_description(self):
        tables = self.tracked_table.get_all()
        columns = self.tracked_column.get_all()

        test_description = 'test description'

        for table in tables.values():
            table.update_snapshot()
            table.close_connection()
            if any([column.home_table == table.name for column in table.extant_columns.values()]):
                # silence print commands, Spoof a series of user inputs
                with mock.patch('sys.stdout', new=StringIO()), mock.patch('auto_doc.run.get_input', side_effect=['N']), mock.patch('auto_doc.run.get_input_from_editor'), mock.patch('auto_doc.run.read_from_editor', side_effect=['']):
                    self.assertFalse(
                        document_columns(
                            table,
                            document_all=True,
                        )
                    )

                for column in table.extant_columns.values():
                    self.assertNotEqual(column.description, test_description)

    def test_document_columns__encoded_column(self):
        tables = self.tracked_table.get_all()
        columns = self.tracked_column.get_all()

        test_description = 'test description'

        for table in tables.values():
            # silence print commands, Spoof a series of user inputs
            with mock.patch('sys.stdout', new=StringIO()), mock.patch('auto_doc.run.get_input', side_effect=['Y', 'new_value_1', '', 'new_value_2', '']), mock.patch('auto_doc.run.get_input_from_editor'), mock.patch('auto_doc.run.read_from_editor', side_effect=[test_description]):
                self.assertTrue(document_columns(table))

            for column in table.extant_columns.values():
                self.assertEqual(column.description, test_description)

                self.assertTrue('new_value_1' in column.documentation['decode_values'])
                self.assertTrue('new_value_2' in column.documentation['decode_values'])
                for decode in column.documentation['decode_values'].values():
                    self.assertEqual(decode, test_description)

    def test_document_columns__encoded_column_extra_values(self):
        tables = self.tracked_table.get_all()
        columns = self.tracked_column.get_all()

        test_description = 'test description'

        for table in tables.values():
            # silence print commands, Spoof a series of user inputs
            with mock.patch('sys.stdout', new=StringIO()), mock.patch('auto_doc.run.get_input', side_effect=['Y']), mock.patch('auto_doc.run.get_input_from_editor'), mock.patch('auto_doc.run.read_from_editor', side_effect=[test_description]):
                self.assertTrue(document_columns(table))

            for column in table.extant_columns.values():
                self.assertEqual(column.description, test_description)

                for decode in column.documentation['decode_values'].values():
                    self.assertEqual(decode, test_description)

    def test_document_columns__encoded_column_no_response(self):
        tables = self.tracked_table.get_all()
        columns = self.tracked_column.get_all()

        test_description = 'test description'

        for table in tables.values():
            for column in table.columns.values():
                column.documentation['decode_values'] = {'not': 'null'}

            # silence print commands
            with mock.patch('sys.stdout', new=StringIO()), mock.patch('auto_doc.run.get_input_from_editor'), mock.patch('auto_doc.run.read_from_editor', side_effect=[test_description]):
                self.assertTrue(document_columns(table))

            for column in table.extant_columns.values():
                self.assertEqual(column.description, test_description)

                for decode in column.documentation['decode_values'].values():
                    self.assertEqual(decode, test_description)

    def test_handle_untracked_input(self):
        new_table_name   = 'new_table'
        import_table_name   = 'import_{table_name}'.format(table_name=new_table_name)
        graph_table_name = 'graph_{table_name}'.format(table_name=new_table_name)

        # silence print commands, Spoof a series of user inputs
        with mock.patch('sys.stdout', new=StringIO()), mock.patch('auto_doc.run.get_input', side_effect=['y']):
            handle_untracked_input(
                [
                    new_table_name,
                    import_table_name,
                    graph_table_name,
                ],
                tracked_types=self.tracked_object.get_tracked_object_types()
            )

        new_table_path = os.path.join(
            self.tracked_table.subclass_file_path,
            new_table_name + '.py',
        )
        self.assertTrue(
            os.path.exists(
                new_table_path,
            )
        )

        import_table_path = os.path.join(
            self.fixed_width_import_table.subclass_file_path,
            import_table_name + '.py',
        )
        self.assertTrue(
            os.path.exists(
                import_table_path,
            )
        )

        graph_table_path = os.path.join(
            self.tracked_graph.subclass_file_path,
            graph_table_name + '.py',
        )
        self.assertTrue(
            os.path.exists(
                graph_table_path,
            )
        )

        os.remove(new_table_path)
        os.remove(import_table_path)
        os.remove(graph_table_path)

    def test_get_tables_to_consider__all(self):
        tracked_objects = {}
        for tracked_type in self.tracked_object.get_tracked_object_types().values():
            tracked_objects.update(tracked_type.get_all())

        tracked_names = set(tracked_objects.keys())

        # silence print commands
        with mock.patch('sys.stdout', new=StringIO()):
            returned_tabs = get_tables_to_consider(
                tracked_names,
                tracked_objects,
                build_all=True,
                build_changed=False,
                recursive=False,
                cascade=False,
            )

        self.assertEqual(returned_tabs, tracked_names)

    def test_get_tables_to_consider__changed(self):
        tracked_objects = {}
        changed_objects = set()
        for tracked_type in self.tracked_object.get_tracked_object_types().values():
            if tracked_type.object_type != 'columns':
                for name, object in tracked_type.get_all().items():
                    object.update_snapshot()
                    object.close_connection()
                    tracked_objects[name] = object

        for name, object in tracked_objects.items():
            object.tear_down()
            object.update_snapshot()
            object.close_connection()
            changed_objects.add(name)

        tracked_names = list(tracked_objects.keys())

        # silence print commands
        with mock.patch('sys.stdout', new=StringIO()):
            returned_tabs = get_tables_to_consider(
                tracked_names,
                tracked_objects,
                build_all=False,
                build_changed=True,
                recursive=False,
                cascade=False,
            )

        for name, object in tracked_objects.items():
            object.close_connection()

        self.assertEqual(returned_tabs, changed_objects)

    def test_get_tables_to_consider__cascade(self):
        tables = self.tracked_table.get_all()
        tracked_objects = self.tracked_table.get_all_tracked_objects()
        tracked_names = ['test_table_2']

        # silence print commands
        with mock.patch('sys.stdout', new=StringIO()):
            returned_tabs = get_tables_to_consider(
                tracked_names,
                tracked_objects,
                build_all=False,
                build_changed=False,
                recursive=False,
                cascade=True,
            )
        self.assertCountEqual(returned_tabs, ['test_table_4', 'test_table_2',])

    def test_get_tables_to_consider__recursive(self):
        tables = self.tracked_table.get_all()
        tracked_objects = self.tracked_table.get_all_tracked_objects()
        tracked_names = ['test_table_2']

        # silence print commands
        with mock.patch('sys.stdout', new=StringIO()):
            returned_tabs = get_tables_to_consider(
                tracked_names,
                tracked_objects,
                build_all=False,
                build_changed=False,
                recursive=True,
                cascade=False,
            )

        self.assertCountEqual(returned_tabs, ['test_table_1', 'test_table_2',])

    def test_output_tables(self):
        pass

    def test_list_tables(self):
        pass


class TrackedGraphTestCase(CommonSetUpTestCase):

    def test_graph_product_path(self):
        graphs = self.tracked_graph.get_all()
        self.assertEqual(
            graphs['graph_test_table_1'].graph_product_path,
            r'R:\working\users\david\auto_doc\auto_doc\tests\graphs\graph_product\graph_test_table_1',
        )

    def test_csv_output_path(self):
        graphs = self.tracked_graph.get_all()
        self.assertEqual(
            graphs['graph_test_table_1'].csv_output_path,
            r'R:\working\users\david\auto_doc\auto_doc\tests\graphs\graph_product\graph_test_table_1\graph_test_table_1.csv',
        )        

    def test_build_code_template(self):
        test_table_name = 'test_table'
        code_template = self.tracked_graph.build_code_template(test_table_name)
        self.assertTrue(isinstance(code_template, str))
        self.assertNotEqual(code_template.find(self.tracked_graph.__name__), -1)
        self.assertNotEqual(code_template.find("name = '{}'".format(test_table_name)), -1)

    def test_finalize__warns_on_failed_build_graph_product(self):
        
        graphs = self.tracked_graph.get_all()

        def auto_failing_func():
            raise Exception("This is an artifical error")

        for graph in graphs.values():
            graph.make_graph_product = auto_failing_func
            with warnings.catch_warnings(record=True) as w:
                graph.finalize()
                self.assertEqual(str(w[0].message), "Supressing error in make_graph_product: This is an artifical error",)

    def test_load_dataframe__creates_correct_csv(self):
        graphs = self.tracked_graph.get_all()

        for graph in graphs.values():
            cursor = graph.get_cursor()
            cursor.execute("SELECT * FROM {graph}".format(graph=graph.name))
            raw_db_data = cursor.fetchall()

            cursor.execute("SELECT column_name FROM information_schema.columns WHERE table_name = '{}'".format(graph.name))
            db_column_names = [column_name_tuple[0] for column_name_tuple in cursor.fetchall()]
            graph.close_connection()

            if os.path.exists(graph.graph_product_path):
                shutil.rmtree(graph.graph_product_path)

            graph.load_dataframe()
            graph.close_connection()

            self.assertTrue(os.path.exists(graph.graph_product_path))

            db_data = []
            for row in raw_db_data:
                row_data = {}
                for column_name, column_data in zip(db_column_names, row):
                    row_data[column_name] = column_data
                db_data.append(row_data)

            file_data = []
            with open(graph.csv_output_path, 'r') as f:
                reader = csv.DictReader(f)
                for line in reader:
                    file_data.append(line)

            self.assertEqual(len(db_data), len(file_data))
            for db_row, file_row in zip(db_data, file_data):
                self.assertCountEqual(db_row, file_row)

    def test_save_plot(self):
        graphs = self.tracked_graph.get_all()

        for graph in graphs.values():
            

            cursor = graph.get_cursor()
            cursor.execute("SELECT column_name FROM information_schema.columns WHERE table_name = '{}'".format(graph.name))
            column_names = [column_name_tuple[0] for column_name_tuple in cursor.fetchall()]
            graph.close_connection()

            test_fig_1_path = os.path.join(graph.graph_product_path, "test_fig_1.pdf")
            test_fig_2_path = os.path.join(graph.graph_product_path, "test_fig_1.pdf")

            self.assertFalse(os.path.exists(test_fig_1_path))
            self.assertFalse(os.path.exists(test_fig_2_path))

            df = graph.load_dataframe()
            graph.close_connection()

            if os.path.exists(graph.graph_product_path):
                shutil.rmtree(graph.graph_product_path)

            df.plot(x=column_names[0], y=column_names[1:])
            graph.save_plot('test_fig_1')

            self.assertTrue(os.path.exists(graph.graph_product_path))
            self.assertTrue(os.path.exists(test_fig_1_path))

            df.plot(x=column_names[0], y=column_names[1:])
            graph.save_plot('test_fig_2')

            self.assertTrue(os.path.exists(test_fig_2_path))


class FixedWidthImportTableTestCase(CommonSetUpTestCase):

    def test_build(self):
        imports = self.fixed_width_import_table.get_all()

        for imp in imports.values():

            imp.tear_down()
            imp.update_snapshot()
            self.assertFalse(imp.exists)

            imp.set_up()
            imp.update_snapshot()
            self.assertTrue(imp.exists)

            cursor = imp.get_cursor()
            cursor.execute("SELECT * FROM {imp}".format(imp=imp.name))
            db_data = cursor.fetchall()
            imp.close_connection()

            file_data = []
            for name, file in sorted(imp.flat_file_paths.items(), key=lambda x: x[0]):
                unzipped_file = file[:-3] # cut off the .gz
                with open(unzipped_file, 'r') as f:
                    for line in f.readlines():
                        line_data = [
                            line[column_def['start_pos'] - 1 : column_def['start_pos'] + column_def['length'] - 1].replace('\\', ' ').strip()
                            for column_def in imp.config.values()
                        ]
                        file_data.append(line_data + [name])

            self.assertEqual(len(db_data), len(file_data))
            for db_row, file_row in zip(db_data, file_data):
                self.assertCountEqual(db_row, file_row)

    def test__set_up_connection(self):
        imports = self.fixed_width_import_table.get_all()

        for imp in imports.values():
            imp._set_up_connection()
            self.assertEqual(imp.encoding, imp._connection.encoding)

    def test_get_cursor_has_correct_attributes(self):
        # This is a copy of TrackedObjectTestCase.test_get_cursor
        # Run run here to make sure that calling self._connection.set_client_encoding in _set_up_connection
        # (which silently closes rolls back the connection) doesn't prevent the cursor from being set up correctly
        objects = self.initable_tracked_object.get_all()
        test_object = objects['test_object_1']
        cursor = test_object.get_cursor()

        self.assertNotEqual(test_object._connection, None)

        cursor.execute("SHOW search_path")
        self.assertEqual(cursor.fetchone()[0], self.tracked_object.schema)
        cursor.execute("SELECT current_user")
        self.assertEqual(cursor.fetchone()[0], self.tracked_object.common_user)

        cursor.close()
        cursor = test_object.get_cursor()

        self.assertFalse(cursor.closed)

        test_object._connection.close()
        cursor = test_object.get_cursor()

        self.assertFalse(test_object._connection.closed)
        self.assertFalse(test_object._cursor.closed)
        test_object.close_connection()

    def test_get_config_file_path(self):
        imports = self.fixed_width_import_table.get_all()

        for imp in imports.values():
            self.assertEqual(
                imp.get_config_file_path(),
                r'R:\working\users\david\auto_doc\auto_doc\tests\fixed_width_imports\config_files',
            )

    def test_config_file_name(self):
        imports = self.fixed_width_import_table.get_all()

        for imp in imports.values():
            self.assertEqual(
                imp.config_file_name,
                os.path.join(
                    r'R:\working\users\david\auto_doc\auto_doc\tests\fixed_width_imports\config_files',
                    "{name}.json".format(name=imp.name),
                ),
            )

    def test_config(self):
        imports = self.fixed_width_import_table.get_all()

        for imp in imports.values():
            with open(imp.config_file_name, 'r') as f:
                file_config = json.load(f)

            # If there's a meta column (that tracks which file a record came from) it won't be in the file.
            # We add it now so that we can compair with imp.columns.
            if imp.full_meta_column_name:
                file_config[imp.full_meta_column_name] = None

            self.assertCountEqual(imp.columns.keys(), file_config.keys())

    def test_documentation_consistent_accross_builds(self):
        # This test was added to catch a problem where loading 
        imports = self.fixed_width_import_table.get_all()
        import_test_1 = imports['import_test_1']

        for i in range(10):
            import_test_1.tear_down()
            import_test_1._config = None
            import_test_1.set_up()
            import_test_1.update_snapshot(query_mode='FORCE')

            self.assertEqual(import_test_1._db_snapshot['column_data']['import_test_1_first_name']['ordinal_position'], 1)
            self.assertEqual(import_test_1._db_snapshot['column_data']['import_test_1_last_name']['ordinal_position'], 2)
            self.assertEqual(import_test_1._db_snapshot['column_data']['import_test_1_age']['ordinal_position'], 3)
            self.assertEqual(import_test_1._db_snapshot['column_data']['import_test_1_file']['ordinal_position'], 4)

    def test_build_code_template(self):
        test_table_name = 'test_table'
        code_template = self.fixed_width_import_table.build_code_template(test_table_name)
        self.assertTrue(isinstance(code_template, str))
        self.assertNotEqual(code_template.find(self.fixed_width_import_table.__name__), -1)
        self.assertNotEqual(code_template.find("name = '{}'".format(test_table_name)), -1)

if __name__ == '__main__':
    errors = []
    tests = unittest.main(exit=False)
    errors += tests.result.failures + tests.result.errors
    print('auto_doc_test', end=' ')
    for error, _msg in tests.result.failures + tests.result.errors:
        print('{klass}.{method}'.format(klass=type(error).__name__, method=error._testMethodName), end=' ')
