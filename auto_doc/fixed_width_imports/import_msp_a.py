from ..auto_doc import FixedWidthImportTable


class ImportMspC(FixedWidthImportTable):
    name = 'import_msp_a'
    encoding = 'WIN1252'
    config_name = 'import_msp_a.json'
    flat_file_paths = {
        '1985-1986': r'R:\DATA\2019-04-25\msp\msp1985-86.A.dat.gz',
        '1986-1987': r'R:\DATA\2019-04-25\msp\msp1986-87.A.dat.gz',
        '1987-1988': r'R:\DATA\2019-04-25\msp\msp1987-88.A.dat.gz',
        '1988-1989': r'R:\DATA\2019-04-25\msp\msp1988-89.A.dat.gz',
        '1989-1990': r'R:\DATA\2019-04-25\msp\msp1989-90.A.dat.gz',
        '1990-1991': r'R:\DATA\2019-04-25\msp\msp1990-91.A.dat.gz',
        '1991-1992': r'R:\DATA\2019-04-25\msp\msp1991-92.A.dat.gz',
        '1992-1993': r'R:\DATA\2019-04-25\msp\msp1992-93.A.dat.gz',
        '1993-1994': r'R:\DATA\2019-04-25\msp\msp1993-94.A.dat.gz',
        '1994-1995': r'R:\DATA\2019-04-25\msp\msp1994-95.A.dat.gz',
        '1995-1996': r'R:\DATA\2019-04-25\msp\msp1995-96.A.dat.gz',
        '1997-1998': r'R:\DATA\2019-04-25\msp\msp1997-98.A.dat.gz',
        '1998-1999': r'R:\DATA\2019-04-25\msp\msp1998-99.A.dat.gz',
        '1999-2000': r'R:\DATA\2019-04-25\msp\msp1999-00.A.dat.gz',
    }

    meta_column_name = "fiscal_year"
