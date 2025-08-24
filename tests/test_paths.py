from pathlib import Path

from bundestag.paths import Paths, get_paths


def test_get_paths():
    root_path_str = "some/test/path"
    paths = get_paths(root_path_str)
    assert isinstance(paths, Paths)
    assert paths.root_path == Path(root_path_str)


def test_paths_init(tmp_path: Path):
    paths = Paths(tmp_path)

    # Test __post_init__
    assert paths.raw_base == tmp_path / "raw"
    assert paths.raw_abgeordnetenwatch == tmp_path / "raw" / "abgeordnetenwatch"
    assert paths.raw_bundestag == tmp_path / "raw" / "bundestag"
    assert paths.raw_bundestag_html == tmp_path / "raw" / "bundestag" / "htm_files"
    assert paths.raw_bundestag_sheets == tmp_path / "raw" / "bundestag" / "sheets"

    assert paths.preprocessed_base == tmp_path / "preprocessed"
    assert (
        paths.preprocessed_abgeordnetenwatch
        == tmp_path / "preprocessed" / "abgeordnetenwatch"
    )
    assert paths.preprocessed_bundestag == tmp_path / "preprocessed" / "bundestag"


def test_make_raw_paths_dry(tmp_path: Path):
    paths = Paths(tmp_path)
    paths.make_raw_paths(dry=True)

    assert not paths.raw_bundestag_html.exists()
    assert not paths.raw_bundestag_sheets.exists()


def test_make_raw_paths(tmp_path: Path):
    paths = Paths(tmp_path)
    paths.make_raw_paths(dry=False)

    assert paths.raw_bundestag_html.exists()
    assert paths.raw_bundestag_sheets.exists()


def test_make_preprocessed_paths_dry(tmp_path: Path):
    paths = Paths(tmp_path)
    paths.make_preprocessed_paths(dry=True)

    assert not paths.preprocessed_abgeordnetenwatch.exists()
    assert not paths.preprocessed_bundestag.exists()


def test_make_preprocessed_paths(tmp_path: Path):
    paths = Paths(tmp_path)
    paths.make_preprocessed_paths(dry=False)

    assert paths.preprocessed_abgeordnetenwatch.exists()
    assert paths.preprocessed_bundestag.exists()


def test_make_paths(tmp_path: Path):
    paths = Paths(tmp_path)
    paths.make_paths()

    assert paths.raw_bundestag_html.exists()
    assert paths.raw_bundestag_sheets.exists()
    assert paths.preprocessed_abgeordnetenwatch.exists()
    assert paths.preprocessed_bundestag.exists()
