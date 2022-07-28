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


class RawGeog(FixedWidthImportTable):
    name = 'import_geog'
    meta_column_name = 'year'
    flat_file_paths = {
        '1989': "R:\\DATA\\2020-07-22_163404\\dip_census_geodata\\calendar\\dip_census_geodata1989.A.dat.gz",
        '1990': "R:\\DATA\\2020-07-22_163404\\dip_census_geodata\\calendar\\dip_census_geodata1990.A.dat.gz",
        '1991': "R:\\DATA\\2020-07-22_163404\\dip_census_geodata\\calendar\\dip_census_geodata1991.A.dat.gz",
        '1992': "R:\\DATA\\2020-07-22_163404\\dip_census_geodata\\calendar\\dip_census_geodata1992.A.dat.gz",
        '1993': "R:\\DATA\\2020-07-22_163404\\dip_census_geodata\\calendar\\dip_census_geodata1993.A.dat.gz",
        '1994': "R:\\DATA\\2020-07-22_163404\\dip_census_geodata\\calendar\\dip_census_geodata1994.A.dat.gz",
        '1995': "R:\\DATA\\2020-07-22_163404\\dip_census_geodata\\calendar\\dip_census_geodata1995.A.dat.gz",
        '1996': "R:\\DATA\\2020-07-22_163404\\dip_census_geodata\\calendar\\dip_census_geodata1996.A.dat.gz",
        '1997': "R:\\DATA\\2020-07-22_163404\\dip_census_geodata\\calendar\\dip_census_geodata1997.A.dat.gz",
        '1998': "R:\\DATA\\2020-07-22_163404\\dip_census_geodata\\calendar\\dip_census_geodata1998.A.dat.gz",
        '1999': "R:\\DATA\\2020-07-22_163404\\dip_census_geodata\\calendar\\dip_census_geodata1999.A.dat.gz",
        '2000': "R:\\DATA\\2020-07-22_163404\\dip_census_geodata\\calendar\\dip_census_geodata2000.A.dat.gz",
        '2001': "R:\\DATA\\2020-07-22_163404\\dip_census_geodata\\calendar\\dip_census_geodata2001.A.dat.gz",
        '2002': "R:\\DATA\\2020-07-22_163404\\dip_census_geodata\\calendar\\dip_census_geodata2002.A.dat.gz",
        '2003': "R:\\DATA\\2020-07-22_163404\\dip_census_geodata\\calendar\\dip_census_geodata2003.A.dat.gz",
        '2004': "R:\\DATA\\2020-07-22_163404\\dip_census_geodata\\calendar\\dip_census_geodata2004.A.dat.gz",
        '2005': "R:\\DATA\\2020-07-22_163404\\dip_census_geodata\\calendar\\dip_census_geodata2005.A.dat.gz",
        '2006': "R:\\DATA\\2020-07-22_163404\\dip_census_geodata\\calendar\\dip_census_geodata2006.A.dat.gz",
        '2007': "R:\\DATA\\2020-07-22_163404\\dip_census_geodata\\calendar\\dip_census_geodata2007.A.dat.gz",
        '2008': "R:\\DATA\\2020-07-22_163404\\dip_census_geodata\\calendar\\dip_census_geodata2008.A.dat.gz",
        '2009': "R:\\DATA\\2020-07-22_163404\\dip_census_geodata\\calendar\\dip_census_geodata2009.A.dat.gz",
        '2010': "R:\\DATA\\2020-07-22_163404\\dip_census_geodata\\calendar\\dip_census_geodata2010.A.dat.gz",
        '2011': "R:\\DATA\\2020-07-22_163404\\dip_census_geodata\\calendar\\dip_census_geodata2011.A.dat.gz",
        '2012': "R:\\DATA\\2020-07-22_163404\\dip_census_geodata\\calendar\\dip_census_geodata2012.A.dat.gz",
        '2013': "R:\\DATA\\2020-07-22_163404\\dip_census_geodata\\calendar\\dip_census_geodata2013.A.dat.gz",
        '2014': "R:\\DATA\\2020-07-22_163404\\dip_census_geodata\\calendar\\dip_census_geodata2014.A.dat.gz",
        '2015': "R:\\DATA\\2020-07-22_163404\\dip_census_geodata\\calendar\\dip_census_geodata2015.A.dat.gz",
        '2016': "R:\\DATA\\2020-07-22_163404\\dip_census_geodata\\calendar\\dip_census_geodata2016.A.dat.gz",
        '2017': "R:\\DATA\\2020-07-22_163404\\dip_census_geodata\\calendar\\dip_census_geodata2017.A.dat.gz",
        '2018': "R:\\DATA\\2020-07-22_163404\\dip_census_geodata\\calendar\\dip_census_geodata2018.A.dat.gz",
        '2019': "R:\\DATA\\2020-07-22_163404\\dip_census_geodata\\calendar\\dip_census_geodata2019.A.dat.gz",
        '2020': "R:\\DATA\\2020-07-22_163404\\dip_census_geodata\\calendar\\dip_census_geodata2020.A.dat.gz",
    }
