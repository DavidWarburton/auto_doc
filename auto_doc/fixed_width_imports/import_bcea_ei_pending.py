from ..auto_doc import FixedWidthImportTable


class ImportBceaEiPending(FixedWidthImportTable):
    name = 'import_bcea_ei_pending'
    flat_file_paths = {'_unused': r'R:\DATA\2020-04-16\ido_sdpr_caseoffices\idosdpr1990-2017.bcea_cases_uipending.A.dat.gz'} # The path(s) to the data file(s) (.dat.gz file)
                             # This is dictionary where the key is the value 
                             # of the meta_column_name for data from this file.

    meta_column_name = None # The name of the column that will be added to
                            # the data to explain which flat file it came 
                            # from. If None, no extra column will be added.

    # Fixed Width Import tables also require you create a config file at 
    # R:\working\users\david\auto_doc\auto_doc\fixed_width_imports\config_files
    # Named import_bcea_ei_pending.json