from ..auto_doc import FixedWidthImportTable


class ImportIdosdprCaseHeader(FixedWidthImportTable):
    name = 'import_idosdpr_case_header'
    flat_file_paths = {
		"2005-06": r'R:\DATA\2019-05-10\ido_sdprV1\header\idosdpr_caseheader2005-06.B.dat.gz',
		"2006-07": r'R:\DATA\2019-05-10\ido_sdprV1\header\idosdpr_caseheader2006-07.B.dat.gz',
		"2007-08": r'R:\DATA\2019-05-10\ido_sdprV1\header\idosdpr_caseheader2007-08.B.dat.gz',
		"2008-09": r'R:\DATA\2019-05-10\ido_sdprV1\header\idosdpr_caseheader2008-09.B.dat.gz',
		"2009-10": r'R:\DATA\2019-05-10\ido_sdprV1\header\idosdpr_caseheader2009-10.B.dat.gz',
		"2010-11": r'R:\DATA\2019-05-10\ido_sdprV1\header\idosdpr_caseheader2010-11.B.dat.gz',
		"2011-12": r'R:\DATA\2019-05-10\ido_sdprV1\header\idosdpr_caseheader2011-12.B.dat.gz',
		"2012-13": r'R:\DATA\2019-05-10\ido_sdprV1\header\idosdpr_caseheader2012-13.B.dat.gz',
		"2013-14": r'R:\DATA\2019-05-10\ido_sdprV1\header\idosdpr_caseheader2013-14.B.dat.gz',
		"2014-15": r'R:\DATA\2019-05-10\ido_sdprV1\header\idosdpr_caseheader2014-15.B.dat.gz',
		"2015-16": r'R:\DATA\2019-05-10\ido_sdprV1\header\idosdpr_caseheader2015-16.B.dat.gz',
		"2016-17": r'R:\DATA\2019-05-10\ido_sdprV1\header\idosdpr_caseheader2016-17.B.dat.gz',
		"2017-18": r'R:\DATA\2019-05-10\ido_sdprV1\header\idosdpr_caseheader2017-18.B.dat.gz',
    }

    meta_column_name = "fiscal_year"
