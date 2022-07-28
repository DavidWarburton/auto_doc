from ..auto_doc import FixedWidthImportTable


class ImportHospN(FixedWidthImportTable):
    name = 'import_hosp_n'
    flat_file_paths = {
        '2009-10': r'R:\DATA\2019-04-25\hospital\hospital2009-10.N.dat.gz',
        '2010-11': r'R:\DATA\2019-04-25\hospital\hospital2010-11.N.dat.gz',
        '2011-12': r'R:\DATA\2019-04-25\hospital\hospital2011-12.N.dat.gz',
        '2012-13': r'R:\DATA\2019-04-25\hospital\hospital2012-13.N.dat.gz',
        '2013-14': r'R:\DATA\2019-04-25\hospital\hospital2013-14.N.dat.gz',
        '2014-15': r'R:\DATA\2019-04-25\hospital\hospital2014-15.N.dat.gz',
        '2015-16': r'R:\DATA\2019-04-25\hospital\hospital2015-16.N.dat.gz',
        '2016-17': r'R:\DATA\2019-04-25\hospital\hospital2016-17.N.dat.gz',
        '2017-18': r'R:\DATA\2019-04-25\hospital\hospital2017-18.N.dat.gz',
    }
    meta_column_name = 'fiscal_year'
