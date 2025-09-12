import tarfile
from pathlib import Path

import pytest

from bundestag.data.download import huggingface


def make_tar(path: Path, name: str, members: dict[str, bytes]) -> Path:
    """Create a tar.gz archive at path/name containing the given members.

    members: mapping of relative member name -> bytes content
    """
    tar_path = path / name
    with tarfile.open(tar_path, "w:gz") as tar:
        for member_name, data in members.items():
            member_path = path / member_name
            member_path.parent.mkdir(parents=True, exist_ok=True)
            member_path.write_bytes(data)
            tar.add(member_path, arcname=member_name)
    return tar_path


@pytest.fixture
def tarballs(tmp_path: Path) -> dict[str, Path]:
    """Create small raw and preprocessed tarballs in a tmp dir and return their paths."""
    # raw.tar.gz will contain a single file raw/readme.txt
    raw = make_tar(tmp_path, "raw.tar.gz", {"raw/readme.txt": b"raw data"})

    # preprocessed.tar.gz will contain preprocessed/readme.txt
    pre = make_tar(
        tmp_path,
        "preprocessed.tar.gz",
        {"preprocessed/readme.txt": b"preprocessed data"},
    )

    return {"raw": raw, "preprocessed": pre}


def test_run_extracts_tarballs(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, tarballs: dict[str, Path]
):
    """Test that huggingface.run downloads (mocked) and extracts both tarballs."""

    dest = tmp_path / "dest"
    dest.mkdir()

    # monkeypatch urlretrieve in the module so only network calls are mocked
    def fake_urlretrieve(url, filename, *args, **kwargs):
        # determine which tarball to copy based on url
        if "raw.tar.gz" in url:
            src = tarballs["raw"]
        elif "preprocessed.tar.gz" in url:
            src = tarballs["preprocessed"]
        else:
            raise RuntimeError("Unexpected url")
        Path(filename).write_bytes(src.read_bytes())
        return (str(filename), None)

    monkeypatch.setattr(huggingface.request, "urlretrieve", fake_urlretrieve)

    # run should extract the two archives into dest
    huggingface.run(dest, dry=False, assume_yes=True)

    # verify extracted files exist
    assert (dest / "raw" / "readme.txt").read_text() == "raw data"
    assert (dest / "preprocessed" / "readme.txt").read_text() == "preprocessed data"
