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

from ..auto_doc import FixedWidthImportTable


class RawMarriages(FixedWidthImportTable):
    name = 'import_marriages'
    flat_file_paths = {
        '1985': r'R:\DATA\2019-12-20-C\marriages\marriages1985.A.dat.gz',
        '1986': r'R:\DATA\2019-12-20-C\marriages\marriages1986.A.dat.gz',
        '1987': r'R:\DATA\2019-12-20-C\marriages\marriages1987.A.dat.gz',
        '1988': r'R:\DATA\2019-12-20-C\marriages\marriages1988.A.dat.gz',
        '1989': r'R:\DATA\2019-12-20-C\marriages\marriages1989.A.dat.gz',
        '1990': r'R:\DATA\2019-12-20-C\marriages\marriages1990.A.dat.gz',
        '1991': r'R:\DATA\2019-12-20-C\marriages\marriages1991.A.dat.gz',
        '1992': r'R:\DATA\2019-12-20-C\marriages\marriages1992.A.dat.gz',
        '1993': r'R:\DATA\2019-12-20-C\marriages\marriages1993.A.dat.gz',
        '1994': r'R:\DATA\2019-12-20-C\marriages\marriages1994.A.dat.gz',
        '1995': r'R:\DATA\2019-12-20-C\marriages\marriages1995.A.dat.gz',
        '1996': r'R:\DATA\2019-12-20-C\marriages\marriages1996.A.dat.gz',
        '1997': r'R:\DATA\2019-12-20-C\marriages\marriages1997.A.dat.gz',
        '1998': r'R:\DATA\2019-12-20-C\marriages\marriages1998.A.dat.gz',
        '1999': r'R:\DATA\2019-12-20-C\marriages\marriages1999.A.dat.gz',
        '2000': r'R:\DATA\2019-12-20-C\marriages\marriages2000.A.dat.gz',
        '2001': r'R:\DATA\2019-12-20-C\marriages\marriages2001.A.dat.gz',
        '2002': r'R:\DATA\2019-12-20-C\marriages\marriages2002.A.dat.gz',
        '2003': r'R:\DATA\2019-12-20-C\marriages\marriages2003.A.dat.gz',
        '2004': r'R:\DATA\2019-12-20-C\marriages\marriages2004.A.dat.gz',
        '2005': r'R:\DATA\2019-12-20-C\marriages\marriages2005.A.dat.gz',
        '2006': r'R:\DATA\2019-12-20-C\marriages\marriages2006.A.dat.gz',
        '2007': r'R:\DATA\2019-12-20-C\marriages\marriages2007.A.dat.gz',
        '2008': r'R:\DATA\2019-12-20-C\marriages\marriages2008.A.dat.gz',
        '2009': r'R:\DATA\2019-12-20-C\marriages\marriages2009.A.dat.gz',
        '2010': r'R:\DATA\2019-12-20-C\marriages\marriages2010.A.dat.gz',
        '2011': r'R:\DATA\2019-12-20-C\marriages\marriages2011.A.dat.gz',
        '2012': r'R:\DATA\2019-12-20-C\marriages\marriages2012.A.dat.gz',
        '2013': r'R:\DATA\2019-12-20-C\marriages\marriages2013.A.dat.gz',
        '2014': r'R:\DATA\2019-12-20-C\marriages\marriages2014.A.dat.gz',
        '2015': r'R:\DATA\2019-12-20-C\marriages\marriages2015.A.dat.gz',
        '2016': r'R:\DATA\2019-12-20-C\marriages\marriages2016.A.dat.gz',
        '2017': r'R:\DATA\2019-12-20-C\marriages\marriages2017.A.dat.gz',
    }
    meta_column_name = 'year'
