from bundestag import html_parsing as hp
from pathlib import Path
import unittest

class TestHTMLParsing(unittest.TestCase):
    def setUp(self):
        # TODO automatic retrieval of appropriate data location
        path = Path(
            "/mnt/c/PetProjects/bundestag/"
        )
        self.html_path = path / 'website_data'
        self.sheet_path = path / 'sheets'
        self.html_file_paths = hp.get_file_paths(self.html_path, pattern=hp.RE_HTM)
        self.sheet_uris = hp.collect_sheet_uris(self.html_file_paths)
        self.file_title_maps = hp.download_multiple_sheets(self.sheet_uris, sheet_path=self.sheet_path, nmax=3)
        self.sheet_files = hp.get_file_paths(self.sheet_path, pattern=hp.RE_FNAME)
        self.df = hp.get_sheet_df(self.sheet_files[0], file_title_maps=self.file_title_maps)
    def test_file_paths(self):
        hp.test_file_paths(self.html_file_paths, self.html_path)
    def test_sheet_uris(self):
        hp.test_sheet_uris(self.sheet_uris)
    def test_file_title_maps(self):
        hp.test_file_title_maps(self.file_title_maps, self.sheet_uris)
    def test_get_sheet_df(self):
        hp.test_get_sheet_df(self.df)
    def test_squished_df(self):
        hp.test_squished_df(hp.get_squished_dataframe(self.df), self.df)