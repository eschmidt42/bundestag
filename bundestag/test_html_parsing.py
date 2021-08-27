from bundestag import html_parsing as hp
from pathlib import Path
import unittest


class TestHTMLParsing(unittest.TestCase):
    @classmethod
    def setUpClass(self):

        self.read_path = Path("./bundestag.de_data")
        self.write_path = Path("./bundestag.de_data_test")
        self.write_path.mkdir(exist_ok=True)
        self.html = "htm_files"
        self.sheet = "sheets"
        self.write_path.mkdir(exist_ok=True)
        (self.write_path / self.sheet).mkdir(exist_ok=True)
        self.dry = False
        self.nmax = 3

        self.html_file_paths = hp.get_file_paths(
            self.read_path / self.html, pattern=hp.RE_HTM
        )
        self.sheet_uris = hp.collect_sheet_uris(self.html_file_paths)

        hp.download_multiple_sheets(
            self.sheet_uris,
            sheet_path=self.write_path / self.sheet,
            nmax=self.nmax,
            dry=self.dry,
        )
        self.file_title_maps = hp.get_file2poll_maps(
            self.sheet_uris, self.write_path / self.sheet
        )

        self.sheet_files = hp.get_file_paths(
            self.write_path / self.sheet, pattern=hp.RE_FNAME
        )

        self.df = (
            hp.get_sheet_df(self.sheet_files[0], file_title_maps=self.file_title_maps)
            if len(self.sheet_files) > 0
            else None
        )

    def test_0_file_paths(self):
        hp.test_file_paths(self.html_file_paths, self.read_path / self.html)

    def test_1_sheet_uris(self):
        hp.test_sheet_uris(self.sheet_uris)

    def test_2_file_title_maps(self):
        hp.test_file_title_maps(self.file_title_maps, self.sheet_uris)

    def test_3_get_sheet_df(self):
        if self.df is not None:
            hp.test_get_sheet_df(self.df)

    def test_4_squished_df(self):
        if self.df is not None:
            tmp = hp.get_squished_dataframe(self.df)
            hp.test_squished_df(tmp, self.df)
