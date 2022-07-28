from ..auto_doc import FixedWidthImportTable


class ImportMspC(FixedWidthImportTable):
    name = 'import_msp_c'
    encoding = 'WIN1252'
    config_name = 'import_msp_c.json'
    flat_file_paths = {
        '1986-1987': r'R:\DATA\2019-04-25\msp\msp1986-87.C.dat.gz',
        '1987-1988': r'R:\DATA\2019-04-25\msp\msp1987-88.C.dat.gz',
        '1988-1989': r'R:\DATA\2019-04-25\msp\msp1988-89.C.dat.gz',
        '1989-1990': r'R:\DATA\2019-04-25\msp\msp1989-90.C.dat.gz',
        '1990-1991': r'R:\DATA\2019-04-25\msp\msp1990-91.C.dat.gz',
        '1991-1992': r'R:\DATA\2019-04-25\msp\msp1991-92.C.dat.gz',
        '1992-1993': r'R:\DATA\2019-04-25\msp\msp1992-93.C.dat.gz',
        '1993-1994': r'R:\DATA\2019-04-25\msp\msp1993-94.C.dat.gz',
        '1994-1995': r'R:\DATA\2019-04-25\msp\msp1994-95.C.dat.gz',
        '1996-1997': r'R:\DATA\2019-04-25\msp\msp1996-97.C.dat.gz',
        '1997-1998': r'R:\DATA\2019-04-25\msp\msp1997-98.C.dat.gz',
        '1998-1999': r'R:\DATA\2019-04-25\msp\msp1998-99.C.dat.gz',
        '1999-2000': r'R:\DATA\2019-04-25\msp\msp1999-00.C.dat.gz',
        '2000-2001': r'R:\DATA\2019-04-25\msp\msp2000-01.C.dat.gz',
        '2001-2002': r'R:\DATA\2019-04-25\msp\msp2001-02.C.dat.gz',
        '2002-2003': r'R:\DATA\2019-04-25\msp\msp2002-03.C.dat.gz',
        '2003-2004': r'R:\DATA\2019-04-25\msp\msp2003-04.C.dat.gz',
        '2004-2005': r'R:\DATA\2019-04-25\msp\msp2004-05.C.dat.gz',
        '2005-2006': r'R:\DATA\2019-04-25\msp\msp2005-06.C.dat.gz',
        '2006-2007': r'R:\DATA\2019-04-25\msp\msp2006-07.C.dat.gz',
        '2007-2008': r'R:\DATA\2019-04-25\msp\msp2007-08.C.dat.gz',
        '2008-2009': r'R:\DATA\2019-04-25\msp\msp2008-09.C.dat.gz',
        '2009-2010': r'R:\DATA\2019-04-25\msp\msp2009-10.C.dat.gz',
        '2010-2011': r'R:\DATA\2019-04-25\msp\msp2010-11.C.dat.gz',
        '2011-2012': r'R:\DATA\2019-04-25\msp\msp2011-12.C.dat.gz',
        '2012-2013': r'R:\DATA\2019-04-25\msp\msp2012-13.C.dat.gz',
        '2013-2014': r'R:\DATA\2019-04-25\msp\msp2013-14.C.dat.gz',
        '2014-2015': r'R:\DATA\2019-04-25\msp\msp2014-15.C.dat.gz',
        '2015-2016': r'R:\DATA\2019-04-25\msp\msp2015-16.C.dat.gz',
        '2016-2017': r'R:\DATA\2019-04-25\msp\msp2016-17.C.dat.gz',
        '2017-2018': r'R:\DATA\2019-04-25\msp\msp2017-18.C.dat.gz',
    }

    meta_column_name = "fiscal_year"
