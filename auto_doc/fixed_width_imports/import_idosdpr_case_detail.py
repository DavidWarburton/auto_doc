from ..auto_doc import FixedWidthImportTable


class ImportIdosdprCaseDetail(FixedWidthImportTable):
    name = 'import_idosdpr_case_detail'
    
    flat_file_paths = {
        "2005-06": r'R:\DATA\2019-05-10\ido_sdprV1\detail\idosdpr_casedetail2005-06.B.dat.gz',
        "2006-07": r'R:\DATA\2019-05-10\ido_sdprV1\detail\idosdpr_casedetail2006-07.B.dat.gz',
        "2007-08": r'R:\DATA\2019-05-10\ido_sdprV1\detail\idosdpr_casedetail2007-08.B.dat.gz',
        "2008-09": r'R:\DATA\2019-05-10\ido_sdprV1\detail\idosdpr_casedetail2008-09.B.dat.gz',
        "2009-10": r'R:\DATA\2019-05-10\ido_sdprV1\detail\idosdpr_casedetail2009-10.B.dat.gz',
        "2010-11": r'R:\DATA\2019-05-10\ido_sdprV1\detail\idosdpr_casedetail2010-11.B.dat.gz',
        "2011-12": r'R:\DATA\2019-05-10\ido_sdprV1\detail\idosdpr_casedetail2011-12.B.dat.gz',
        "2012-13": r'R:\DATA\2019-05-10\ido_sdprV1\detail\idosdpr_casedetail2012-13.B.dat.gz',
        "2013-14": r'R:\DATA\2019-05-10\ido_sdprV1\detail\idosdpr_casedetail2013-14.B.dat.gz',
        "2014-15": r'R:\DATA\2019-05-10\ido_sdprV1\detail\idosdpr_casedetail2014-15.B.dat.gz',
        "2015-16": r'R:\DATA\2019-05-10\ido_sdprV1\detail\idosdpr_casedetail2015-16.B.dat.gz',
        "2016-17": r'R:\DATA\2019-05-10\ido_sdprV1\detail\idosdpr_casedetail2016-17.B.dat.gz',
        "2017-18": r'R:\DATA\2019-05-10\ido_sdprV1\detail\idosdpr_casedetail2017-18.B.dat.gz',
    }

    meta_column_name = None