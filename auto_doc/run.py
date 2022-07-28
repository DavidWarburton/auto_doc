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

import argparse
import pathlib
import subprocess
import json
import os
import pprint
import string
import csv
import sys
import signal
import xlrd

from copy import copy
from collections import OrderedDict
from . import settings
from .auto_doc import TrackedObject, TrackedColumn, TrackedGraph

TEMP_INPUT_PATH = os.path.join(os.path.dirname(__file__), '.temp_user_input.txt')


parser = argparse.ArgumentParser(description="Track tables so they can be rebuilt automatically")
parser.add_argument(
    'tables',
    metavar='N',
    type=str,
    nargs='*',
    help="""
        The list of tables to build.
    """,
)
parser.add_argument(
    '-a',
    '--all',
    action='store_true',
    dest='build_all',
    help="""
        Attempt to build all tracked tables.
    """,
)
parser.add_argument(
    '-d',
    '--document',
    action='store_true',
    dest='document_only',
    help="""
        Don't rebuild tables, just change documentation for them.
    """
)
parser.add_argument(
    '-edp',
    '--existing_documentation_path',
    action='store',
    dest='existing_documentation_path',
    help="""
        Provide a path to an existing documentation file to import from.
    """
)
parser.add_argument(
    '-f',
    '--force-rebuild',
    action='store_true',
    dest='force_rebuild',
    help="""
        If included, all tables will be torn down and rebuilt.
        Default behavior only rebuilds tables if changes are detected
        in information_schema.tables or information_schema.columns.
    """,
)
parser.add_argument(
    '-ls',
    '--list',
    action='store_true',
    dest='list',
    help="""
        Rather than doing any database actions, just list descriptions and
        dependencies for all listed tables, if they're tracked. Works with -a.
    """,
)
parser.add_argument(
    '-la',
    '--list-all',
    action='store_true',
    dest='list_all',
    help="""
        As -ls, but also show table and column data.
    """,
)
parser.add_argument(
    '-g',
    '--graph',
    action='store_true',
    dest='graph',
    help="""
        Rather than doing any database actions, just run make_graph_product
        for all listed graphs. If this option is present, non-graphs are ignored.
    """,
)
parser.add_argument(
    '-ch',
    '--changed',
    action='store_true',
    dest='build_changed',
    help="""
        Like --all, but lists or builds only tables that show as changed.
    """,
)
parser.add_argument(
    '-r',
    '--recursive',
    action='store_true',
    dest='recursive',
    help="""
        For each listed table, consider all its dependencies as well.
    """,
)
parser.add_argument(
    '-c',
    '--cascade',
    action='store_true',
    dest='cascade',
    help="""
        For each listed table, consider all tables that depend on it as well.
    """,
)
parser.add_argument(
    '-o',
    '--output',
    type=str,
    dest='output',
    help="""
        Create a CSV file with the name and descriptions for all columns
        in the indicated tables.
    """
)


TABLE_DESCRIPTION_TEMPLATE = (
    "# ----------{table}----------\n"
    "# The table {table} has changed since it was last built, please update its description\n"
    "# Simply enter it here, then save this document and close it when you're done.\n"
    "# Lines preceeded by a '#' will be ignored\n"
)

DEPENDENCY_TEMPLATE = (
    "# ----------{table}----------\n"
    "# Which tables were used to create {table}.\n"
    "# Remove the '#' from each tablename below if it was used.\n"
    "# If you can't find the name of a table in this list, that's\n"
    "# because it's not yet tracked. If this table depends on any\n"
    "# untracked tables, then please create tracking information for\n"
    "# those tables. You can then edit the dependencies for this\n"
    "# table with 'auto_doc {table} -d'.\n"
)

COLUMN_DESCRIPTION_TEMPLATE = (
    "# ----------{table}----------\n"
    "# ----------{column}----------\n"
    "# Update the description for column {column} on {table}.\n"
    "# Simply enter it here, then save this document and close it when you're done.\n"
    "# Lines preceeded by a '#' will be ignored\n"
)

COLUMN_VALUE_TEMPLATE = (
    "# ----------{table}----------\n"
    "# ----------{column}----------\n"
    "# ----------{value}----------\n"
    "# What does it mean when {column} takes the value '{value}'?\n"
    "# Enter your answer here, then save this document and close it when you're done.\n"
    "# Lines preceeded by a '#' will be ignored\n"
)


class DependecyError(Exception):
    pass


class InputHandler():
    existing_documentation_path = None
    tracked_objects = None
    no_input = False

    def __init__(
        self,
        existing_documentation_path=None,
        tracked_objects=None,
        no_input=False,
    ):
        self.existing_documentation_path = existing_documentation_path
        self.tracked_objects = tracked_objects
        self.no_input = no_input

    def get_input_from_editor(self, prompt, starting_text):
        with open(TEMP_INPUT_PATH, 'w') as temp_file:
            temp_file.truncate()
            temp_file.write(prompt)
            temp_file.write(starting_text)

        if not self.no_input:
            subprocess.call([TEMP_INPUT_PATH], shell=True)

    def read_from_editor(self):
        user_input = ''
        with open(TEMP_INPUT_PATH, 'r', encoding='latin-1', errors='backslashescape') as temp_file:
            for line in temp_file:
                fixed_line = "".join(filter(lambda x: x in string.printable, line))
                if not fixed_line.strip().startswith('#'):
                    user_input += fixed_line
        return user_input

    def get_input(self):
        if not self.no_input:
            return input('~')
        else:
            return ""

    def get_column_home_table_from_column_name(self, column):
        table_names = list(self.tracked_objects.keys())
        table_names.sort(key=len)
        table_names.reverse()

        for table_name in table_names:
            if column.name.startswith(table_name):
                ret_val = table_name
                break
            else:
                ret_val = ""

        return ret_val

    def get_table_documentation(self, table_to_document):
        if not self.existing_documentation_path:
            self.get_input_from_editor(
                TABLE_DESCRIPTION_TEMPLATE.format(table=table_to_document.name),
                table_to_document.description,
            )
            return self.read_from_editor()
        else:
            with xlrd.open(self.existing_documentation_path):
                pass

    def get_column_home_table(self, column=None):
        if column:
            return self.get_column_home_table_from_column_name(column)
        else:
            return self.get_input()

    def get_new_column_name(self):
        return self.get_input()

    def get_replaced_column_name(self):
        return self.get_input()

    def get_column_documentation(self, table_to_document, column):
        self.get_input_from_editor(
            COLUMN_DESCRIPTION_TEMPLATE.format(
                table=table_to_document.name,
                column=column.name,
            ),
            column.description,
        )
        return self.read_from_editor()

    def is_column_coded(self, column):
        if not self.existing_documentation_path:
            return self.get_input()
        else:
            pass # read existing docs

    def is_column_missing_decode_values(self, column):
        if not self.existing_documentation_path:
            return self.get_input()
        else:
            pass # read existing docs, return "YES" if column.possible_values does not have all values from docs, "NO" otherwise

    def get_missing_decode_value(self, column):
        if not self.existing_documentation_path:
            return self.get_input()
        else:
            pass # read existing docs, return the first value not in column.possible_values

    def confirm_missing_decode_value(self):
        if not self.existing_documentation_path:
            return self.get_input()
        else:
            return "YES"
        
    def get_decode_value_documentation(self, table_to_document, column, value):
        get_input_from_editor(
            COLUMN_VALUE_TEMPLATE.format(
                value=value,
                table=column.name,
                column=column.name,
            ),
            column.documentation['decode_values'].get(value, ''),
        )
        return read_from_editor()

    def confirm_create_tracking_templates(self):
        return self.get_input()


def order_tables_by_dependencies(tables_to_consider, tracked_objects):
    """# Take tables out of tables_to_consider
    # one at a time once we verify they have no dependencies left that
    # haven't been put into ordered_tables ahead of them.
    ordered_tables = []

    
    # If we ever complete one full loop without moving any tables into
    # ordered tables, then the remaining tables have unresolvable dependency
    # issues. Set up a variable to track if we've made a full loop.
    full_loop_table = None
    """

    tables_to_order = OrderedDict(
        (table, tracked_objects[table].get_dependencies(tracked_objects=tracked_objects))
        for table in tables_to_consider
    )
    ordered_tables = []

    while tables_to_order:
        next_tables_to_order = OrderedDict()
        ordered_tables_this_loop = []

        for table, dependencies in tables_to_order.items():
            unresolved_dependencies = []

            for dependency_info in dependencies:
                # Go through the table's dependencies to find the unresolved ones.
                # A dependency is resolved if it is either:
                #   1) Unchanged in the database and not in tables_to_consider
                #   or 2) Has already been added to ordered_tables
                if dependency_info['ready'] and dependency_info['name'] not in tables_to_consider:
                    pass
                elif dependency_info['name'] in ordered_tables:
                    pass
                else:
                    unresolved_dependencies.append(dependency_info)

            if not unresolved_dependencies:
                # If the table has no dependencies left it can be put in ordered_tables
                ordered_tables.append(table)
                ordered_tables_this_loop.append(table)
            else:
                # Otherwise, we'll have to try again the next time through
                next_tables_to_order[table] = unresolved_dependencies

        if not ordered_tables_this_loop:
            # If no tables were put into ordered_tables this loop, then 
            # 
            error_message = "Dependency issues detected:\n"

            for table_name, dependencies in tables_to_order.items():
                table_dependency_issues = ''

                for dependency_info in dependencies:
                    if not dependency_info['ready']:
                        table_dependency_issues += str.join('', [
                            " "*4,
                            dependency_info['name'],
                            '\n',
                        ])
                        table_dependency_issues += str.join('', [
                            " "*8,
                            "exists: ",
                            str(dependency_info['exists']),
                            '\n',
                        ])
                        table_dependency_issues += str.join('', [
                            " "*8,
                            "ready: ", 
                            str(dependency_info['ready']),
                            '\n',
                        ])

                error_message += str.join('', [table_name, '\n',])
                error_message += (table_dependency_issues)

            raise DependecyError(error_message)

        tables_to_order = next_tables_to_order
        
    return ordered_tables  


def build_tables(tables_to_consider, tracked_objects, force_rebuild=False):
    results = {
        table_name: False
        for table_name in tables_to_consider
    }

    for table_name in tables_to_consider:
        try:
            for dependency in tracked_objects[table_name].get_dependencies(tracked_objects=tracked_objects):
                if isinstance(results[dependency['name']], Exception):
                    print("Skipping {table} due to errors building its dependencies".format(table=table_name))
                    break
            else:
                print("Building {table}".format(table=table_name))
                if force_rebuild:
                    tracked_objects[table_name].tear_down()

                #try:
                tracked_objects[table_name].set_up()
                #except Exception as e:
                #    results[table_name] = e
                #    print("ERROR", e)
                #else:
                tracked_objects[table_name].finalize()
                results[table_name] = True
                print("DONE")
                print('--------------------')

        except KeyboardInterrupt:
            print(
                "\n"
                "Rolling back current table due to KeyboardInterrupt.\n" 
                "Please enter documentation for any tables that have already finished building.\n"
            )
            break
    else:
        print("All builds complete.")

    return results


def output_tables(
    tables_to_consider,
    tracked_objects, 
    path,
):

    try:
        dirname, _filename = os.path.split(path)
    except:
        print(
            "File path {path} is invalid. Enter a valid path.".format(
                path=path,
            )
        )
    else:
        if not os.path.exists(dirname):
            print(
                "Destination folder ({dirname}) does not exist.".format(
                    dirname=dirname,
                )
            )
        
        if os.path.exists(path):
            print(
                "File already exists at destination ({path})".format(
                    path=path
                )
            )

    with open(path, 'w') as csv_output_file:
        csv_columns = ["Variable Name", "Description"]
        writer = csv.DictWriter(csv_output_file, csv_columns)
        for table_name in sorted(tables_to_consider):
            for column in tracked_objects[table_name].extant_columns.values():
                writer.writerow({
                    csv_columns[0]: column.name,
                    csv_columns[1]: column.description,
                })

    print("Column names and descriptions saved at {path}".format(
            path=path,
        )
    )


def list_tables(tables_to_consider, tracked_objects, list_all=False):
    for table_name in sorted(tables_to_consider):
        if not hasattr(tracked_objects[table_name], 'table_changed'):
            tracked_objects[table_name].update_snapshot()
        print(
            "{table}\n"
            "  dependencies: {dependencies}".format(
                table=table_name,
                dependencies=tracked_objects[table_name].documentation['dependencies'],
            )
        )
        print(
            '  table_changed: ' + str(tracked_objects[table_name].table_changed),
            '  column_changed: ' + str(tracked_objects[table_name].column_changed),
            '  size_changed: ' + str(tracked_objects[table_name].size_changed), 
        )

        if list_all:
            for line in tracked_objects[table_name].description.split("\n"):
                print("    ", line)

            print(
                "\n"
                "  Columns"
            )

            for column in tracked_objects[table_name].extant_columns.values():
                print("    ", column.name)
                for line in column.description.split("\n"):
                    print("      ", line)

                print(
                    "      ",
                    tracked_objects[table_name].documentation['column_data'][column.name],
                    "\n",
                )


def graph_tables(tables_to_consider, tracked_objects):
    for table_name in tables_to_consider:
        if table_name.startswith(TrackedGraph.prefix):
            print("Graphing {name}".format(name=table_name))
            tracked_objects[table_name].make_graph_product()
    print("All graphs complete")


def document_tables(tables_to_consider, tracked_objects, document_all=False):
    # Go through all tracked tables and check for changes.
    for table_name in tables_to_consider:
        tracked_objects[table_name].update_snapshot()

        tracked_columns = TrackedColumn.get_all()
        if (
            tracked_objects[table_name].exists and
            (document_all or tracked_objects[table_name].changed)
        ):
            if not document_description(tracked_objects[table_name], tracked_objects):
                continue

            if hasattr(tracked_objects[table_name], 'extant_columns'):
                rename_columns(
                    table_name,
                    tracked_objects,
                )

                if not document_columns(
                    tracked_objects[table_name],
                    tracked_objects,
                    document_all=document_all,
                ):
                    continue

                if not document_possible_values(
                    tracked_objects[table_name],
                    tracked_objects,
                    document_all=document_all,
                ):
                    continue

            print("Finalizing {table}".format(table=table_name))

            tracked_objects[table_name].update_snapshot()
            if tracked_objects[table_name].changed:
                tracked_objects[table_name].finalize()
            else:
                tracked_objects[table_name].close_connection()
            tracked_objects[table_name].update_documentation_file(tracked_objects=tracked_objects)


def document_description(
    table_to_document,
    tracked_objects=None,
):
    """
    collect a description from the user
    return False if they fail to provide one, True otherwise
    """

    print(
        "Please update documentation for {table}. "
        "A text editor should open for this purpose.".format(
            table=table_to_document.name,
        )
    )
    table_to_document.description = input_handler.get_table_documentation(table_to_document)

    if not table_to_document.description:
        print("Skipping to next table due to empty description.")
        return False

    table_to_document.update_documentation_file(
        tracked_objects=tracked_objects,
        insert_self_into_descendents=False,
    )

    return True


def rename_columns(
    table_name,
    tracked_objects=None,
):

    deleted_columns = {}
    for column in tracked_objects[table_name].columns.values():
        if (
            not column.exists and
            column.name not in tracked_objects[table_name].documentation['renamed_columns']
        ):
            deleted_columns[column.name] = column

    for column in sorted(tracked_objects[table_name].extant_columns.values(), key=lambda x: x.name):
        new_home_table = None
        new_column_name = None
        old_column_name = None

        if not column.name_fits_convention() or (
            column.home_table != table_name and
            column.name not in tracked_objects[column.home_table].extant_columns
        ):
            print(
                "{column_name} does not match naming convention. "
                "What table is it from? (enter nothing if it's from {table_name})".format(
                    table_name=table_name,
                    column_name = column.name,
                )
            )

            while new_home_table not in tracked_objects:
                new_home_table = input_handler.get_column_home_table(column)
                if new_home_table == '':
                    new_home_table = table_name
            column.home_table = new_home_table

            default_name = column.fit_name_to_convention()
            print(
                "Enter a new name for {column_name} begining "
                "with '{home_table}_' (enter nothing to use '{default_name}').".format(
                    default_name=default_name,
                    column_name=column.name,
                    home_table=column.home_table,
                )
            )

            if not tracked_objects[column.home_table].is_updated:
                tracked_objects[column.home_table].update_snapshot()
                tracked_objects[column.home_table].close_connection()

            while (
                (
                    not column.name_fits_convention(new_column_name)
                ) or (
                    column.home_table != table_name and
                    new_column_name not in tracked_objects[column.home_table].extant_columns
                )
            ):
                new_column_name = input_handler.get_new_column_name()

                if new_column_name == '':
                    new_column_name = default_name

        if (
            deleted_columns and
            not column.description and
            column.home_table == table_name
        ):
            print(
                "The following columns have been removed from the table\n"
                "{columns}\n"
                "Is {new_column} a renaming of any of these? "
                "If so, enter the old column name. Enter 'N' or 'No' or nothing otherwise.".format(
                    columns=', '.join(deleted_columns.keys()),
                    new_column=new_column_name or column.name,
                )
            )
            while old_column_name not in deleted_columns:
                old_column_name = input_handler.get_replaced_column_name()

                if old_column_name.upper() in {'', 'N', 'NO'}:
                    old_column_name = None
                    break
                elif old_column_name in deleted_columns:
                    deleted_columns.pop(old_column_name, None)
                    break

        if new_column_name and new_column_name != column.name:
            tracked_objects[table_name].rename_column(
                column.name,
                new_column_name,
            )

        if (old_column_name or column.name) != (new_column_name or column.name):
            tracked_objects[table_name].rename_column(
                old_column_name or column.name,
                new_column_name or column.name,
            )
            if tracked_objects[table_name].columns[old_column_name or column.name].exists:
                tracked_objects[table_name].change_column_name_in_database(
                    old_column_name or column.name,
                    new_column_name or column.name,
                )

        tracked_objects[table_name].update_snapshot()
        tracked_objects[table_name].update_documentation_file(
            tracked_objects=tracked_objects,
            insert_self_into_descendents=False,
        )

def document_columns(
    table_to_document,
    tracked_objects=None,
    document_all=False,
):
    print(
        "Please update descriptions "
        "for each column in {table}.".format(
            table=table_to_document.name
        )
    )

    for column in sorted(table_to_document.extant_columns.values(), key=lambda x: x.name):
        if (
            column.home_table == table_to_document.name and
            (document_all or column.changed)
        ):
            column.description = input_handler.get_column_documentation(table_to_document, column)

            if not column.description:
                print("Skipping table due to empty column description")
                return False

            table_to_document.update_documentation_file(
                tracked_objects=tracked_objects,
                insert_self_into_descendents=False,
            )

    return True


def document_possible_values(
    table_to_document,
    tracked_objects=None,
    document_all=False,
):

    for column in sorted(table_to_document.extant_columns.values(), key=lambda x: x.name):
        if (
            column.home_table == table_to_document.name and
            (document_all or column.changed)
        ):
            if column.is_coded is not False:
                extra_values = set()
                if not column.is_coded:
                    print("Column {column_name} has relatively few possible values, is it a coded column (Y/n)?".format(
                            column_name=column.name
                        )
                    )
                    response = input_handler.is_column_coded(column)

                if column.is_coded or response.upper() in ['Y', 'YES']:
                    print(
                        "Column {column_name} is estimated to have the following possible values\n"
                        "{values}\n"
                        "Are any missing (Y/n)?".format(
                            column_name=column.name,
                            values=column.possible_values,
                        )
                    )
                    response = input_handler.is_column_missing_decode_values(column)
                    if response.upper() in ['Y', 'YES']:
                        response = 'continue'
                    else:
                        response = None

                    while response:
                        print("Type a missing value (current values are {values})".format(values=column.possible_values.union(extra_values)))
                        response = input_handler.get_missing_decode_value(column)

                        if response:
                            print("{response} will be added to possible values? (Blank or Y for yes, n for no.)".format(response=response))
                            if input_handler.confirm_missing_decode_value():
                                extra_values.add(response)

                    possible_value_dict = dict.fromkeys(column.possible_values.union(extra_values), '')
                    possible_value_dict.update(column.documentation['decode_values'])
                    column.documentation['decode_values'] = possible_value_dict
                    table_to_document.update_documentation_file(
                        tracked_objects=tracked_objects,
                        insert_self_into_descendents=False,
                    )

                    for value in column.documentation['decode_values']:
                        column.documentation['decode_values'][value] = input_handler.get_decode_value_documentation(
                            table_to_document,
                            column,
                            value,
                        )
                        table_to_document.update_documentation_file(
                            tracked_objects=tracked_objects,
                            insert_self_into_descendents=False,
                        )
                else:
                    column.documentation['decode_values'] = {}

    table_to_document.update_documentation_file(
        tracked_objects=tracked_objects,
        insert_self_into_descendents=False,
    )


def handle_untracked_input(
    tables_to_consider,
    response=None,
    tracked_types=None,
):
    if not tracked_types:
        tracked_types = TrackedObject.get_tracked_object_types()
    tracked_types = sorted(
        [
            object_type
            for object_type in tracked_types.values()
            if object_type.prefix != None
        ],
        key=lambda type: -len(type.prefix)
    )

    print(
        "The following tables are untracked: {untracked_objects}."
        "Would you like to create tracking templates for them (Y/n)?".format(
            untracked_objects=tables_to_consider,
        )
    )
    response = input_handler.confirm_create_tracking_templates()
    if not response or response.upper() not in ['Y', 'YES']:
        return

    for table_name in tables_to_consider:

        for tracked_type in tracked_types:
            if table_name.startswith(tracked_type.prefix):
                tracked_object_type = tracked_type
                break

        with open(os.path.join(tracked_object_type.subclass_file_path, table_name + '.py'), 'w') as template:
            template.write(
                tracked_object_type.build_code_template(table_name)
            )
            print(
                "Table {table} is not tracked. A tracking template has been created at {location}.".format(
                    table=table_name,
                    location=template.name,
                )
            )


def get_tables_to_consider(
    table_names,
    tracked_objects,
    build_all,
    build_changed,
    recursive,
    cascade,
):
    tables_to_consider = set(table_names)

    if build_all:
        tables_to_consider = tables_to_consider.union(tracked_objects.keys())

    if build_changed:
        changed_tables = set()
        for table_name, tracked_table in tracked_objects.items():
            tracked_table.update_snapshot()
            tracked_table.close_connection()
            if tracked_table.changed:
                changed_tables.add(table_name)
        tables_to_consider = tables_to_consider.union(changed_tables)

    if not tables_to_consider:
        parser.print_help()
        exit()

    tracked_objects_to_consider = set(
        table_name
        for table_name in tables_to_consider
        if table_name in tracked_objects
    )

    untracked_objects_to_consider = tables_to_consider - tracked_objects_to_consider

    if untracked_objects_to_consider:
        handle_untracked_input(untracked_objects_to_consider)

    if recursive:
        for table_name in copy(tracked_objects_to_consider):
            for dependency_info in tracked_objects[table_name].get_dependencies(tracked_objects=tracked_objects):
                tracked_objects_to_consider.add(dependency_info['name'])

    if cascade:
        for table_name in copy(tracked_objects_to_consider):
            for descendent_info in tracked_objects[table_name].get_descendents(tracked_objects=tracked_objects):
                tracked_objects_to_consider.add(descendent_info['name'])

    return tracked_objects_to_consider


if __name__ == "__main__":
    args = parser.parse_args()

    def raise_keyboard_interupt(*args, **kwargs):
        raise KeyboardInterrupt

    signal.signal(signal.SIGINT, raise_keyboard_interupt)
    signal.signal(signal.SIGBREAK, raise_keyboard_interupt)

    tracked_objects = {}
    for tracked_type in TrackedObject.get_tracked_object_types().values():
        if tracked_type.buildable:
            tracked_objects.update(tracked_type.get_all())

    tracked_objects_to_consider = get_tables_to_consider(
        args.tables,
        tracked_objects,
        args.build_all,
        args.build_changed,
        args.recursive,
        args.cascade,
    )

    if not (args.list or args.list_all or args.document_only or args.output or args.graph):
        if len(tracked_objects_to_consider) > 1:
            tracked_objects_to_consider = order_tables_by_dependencies(
                tracked_objects_to_consider,
                tracked_objects,
            )
        build_results = build_tables(
            tracked_objects_to_consider,
            tracked_objects,
            force_rebuild=args.force_rebuild,
        )

        tracked_objects_to_consider = set(tracked_objects_to_consider).intersection(
            set(table_name for table_name, result in build_results.items() if result is True)
        )

    elif args.list or args.list_all:
        list_tables(tracked_objects_to_consider, tracked_objects, list_all=args.list_all)
    elif args.output:
        output_tables(tracked_objects_to_consider, tracked_objects, args.output)
    elif args.graph:
        graph_tables(tracked_objects_to_consider, tracked_objects)

    if not (args.list or args.list_all or args.output):
        document_tables(tracked_objects_to_consider, tracked_objects, document_all=args.document_only)
