Auto_doc is a tool for tracking how data is transformed or created in the course of research.

## Features

	Contains tools for importing data from fixed width files to PostgreSQL.
	Provides abstracted functions for connecting to the database.
	Provides a central storage place for code relating to building tables.
	Automatically collects meta-data on any tables built with it.
	Enforces naming conventions for tables and columns
	Requires short descriptions for all tables and columns
	For columns with relatively few possible values, requires users record the meaning of each value if applicable

## Usage

Auto_doc uses a command line interface. The auto_doc --help is available to explain all available flags.

To walk through some basic use cases:

	1) Importing a fixed width file or files into PostgreSQL
	
	Begin with the command auto_doc import_{name of table}
	This will create a template auto_doc/fixed_width_imports/{name of table}.py
	Open up this template and change flat_file_paths, meta_column_name, and encoding as needed (the template will have basic instructions for how these variables work)
	Navigate to auto_doc\auto_doc\fixed_width_imports\config_files and create a file called {name_of_table}.json
	Populate this json file with dictionaries describing each column in the flat file.
		
		Should look similar to 
		{
			"{name of table}_{name of first column}": {
				"start_pos": 1,
				"length": 1
			},
			"{name of table}_{name of second column}": {
				"start_pos": 2,
				"length": 10
			}
		}

	Make sure each column name starts with the name of the table, as this is the naming convention for columns.
	This is to ensure that if two different datasets report the same variable (gender is a common point of overlap) then they are still properly separated in our documentation.
	If this is not done, it will have to be corrected when the table is actually created in PostgreSQL and entered into documentation, requiring some extra input from the user at that stage

	Run auto_doc import_{name_of_table} again.
	This time, since auto_doc/fixed_width_imports/{name of table}.py exists, it will import your flat file or files.
	You will be prompted to input a description of the table and its columns.
	These descriptions are entered one at a time, and if anything goes wrong, everything you've already entered, and the PostgreSQL table itself, will be saved.
	If you are interupted at this point, please come back later and run auto_doc import_{name of table} -d to finish updating the documentation

	2) Creating a basic table

	Begin with auto_doc {name of table}
	This time (because the 'import_' prefix was omitted) a template will be created at auto_doc/tables/{name of table}.py
	Open this template, and start writing the code that will build your table

		This code has to follow two rules:
			1) It must create a table with the name {name of table}. That is to say the name of the table in the database must match the name attribute defined in your file.
			2) It can only create this one table. If you need to create other intermediary tables, consider making files to track them. If they aren't useful to keep around and document, then make them temporary tables.

	The template code sets up a cursor object, and your code should use this to connect to the database
	This is a cursor object as defined by psycopg2, check their docs for details
	Don't worry about closing this cursor or the connection its attached to when you're done, auto_doc will handle that

	Once your code is done you can used auto_doc {name of table} to run it
	Once it completes successfully you'll be prompted to enter documentation and fix column names so they conform to the naming convention

## Requirements

	Python 3.5.3
	pandas==0.25.3
	psycopg2==2.7.7
	PostgreSQL 11.12

## Installation

	Grab this git repository
	Update settings with the credentials needed to connect to your PostgreSQL database
	Done

## Project Status

## Goals/Roadmap

## Getting Help or Reporting an Issue

## How to Contribute

## License
Copyright 2021 Province of British Columbia

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at 

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
