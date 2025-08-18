import typing as T
from pathlib import Path
from unittest.mock import MagicMock, call, patch

import pandas as pd
import pytest
import xlrd

import bundestag.data.transform.bundestag_sheets as transform_bs
import bundestag.data.utils as data_utils
import bundestag.schemas as schemas


def test_get_file2poll_maps():
    uris = {
        "title1": "uri/1.xlsx",
        "title2": "uri/2.xlsx",
    }
    sheet_path = Path("dummy/path")
    known_sheets = [sheet_path / "1.xlsx"]

    with patch(
        "bundestag.data.utils.get_file_paths",
        MagicMock(return_value=known_sheets),
    ) as get_file_paths:
        # line to test
        file2poll = transform_bs.get_file2poll_maps(uris, sheet_path)

        assert len(file2poll) == 1
        assert "1.xlsx" in file2poll
        assert file2poll["1.xlsx"] == "title1"


def test_is_date():
    assert transform_bs.is_date("123", pd.to_datetime) == False
    assert transform_bs.is_date("2022-02-02", pd.to_datetime) == True


@pytest.mark.parametrize(
    "st_size,expected",
    [(st_size, expected) for st_size, expected in zip([0, 1], [True, False])],
)
def test_file_size_is_zero(st_size: int, expected: bool):
    path = Path("dummy/path")
    with patch(
        "pathlib.Path.stat", MagicMock(return_value=MagicMock(st_size=st_size))
    ) as stat:
        # line to test
        assert transform_bs.file_size_is_zero(path) == expected


@pytest.mark.parametrize(
    "case,side_effect,expected",
    [
        (0, (pd.DataFrame({"a": [1]}),), pd.DataFrame({"a": [1]})),
        (1, (ValueError, pd.DataFrame({"a": [1]})), pd.DataFrame({"a": [1]})),
        (2, xlrd.XLRDError, None),
    ],
)
def test_read_excel(case: int, side_effect, expected):
    path = Path("dummy/path")

    with patch("pandas.read_excel", MagicMock(side_effect=side_effect)) as read_excel:
        # line to test
        res = transform_bs.read_excel(path)

        if case == 0:
            assert res.equals(expected)
            read_excel.assert_called_once_with(path, sheet_name=None, engine="xlrd")
        elif case == 1:
            assert res.equals(expected)
            read_excel.assert_has_calls(
                [
                    call(path, sheet_name=None, engine="xlrd"),
                    call(path, sheet_name=None, engine="openpyxl"),
                ]
            )
        elif case == 2:
            assert res is None
            read_excel.assert_called_once_with(path, sheet_name=None, engine="xlrd")


@pytest.mark.parametrize(
    "file_size_is_zero,n_dfs,validate,invalid_votes,file_title_maps_is_none",
    [
        (
            file_size_is_zero,
            n_dfs,
            validate,
            invalid_votes,
            file_title_maps_is_none,
        )
        for file_size_is_zero in [True, False]
        for n_dfs in [0, 1, 2]
        for validate in [True, False]
        for invalid_votes in [True, False]
        for file_title_maps_is_none in [True, False]
    ],
)
def test_get_sheet_df_with_mock(
    file_size_is_zero: bool,
    n_dfs: int,
    validate: bool,
    invalid_votes: bool,
    file_title_maps_is_none: bool,
):
    sheet_file = Path("some/file.xlsx")
    sheet_name = "wup"
    date = pd.to_datetime("1950-01-01")
    title = "wup"
    full_title = title
    file_title_maps = (
        None
        if file_title_maps_is_none
        else {
            "file.xlsx": "wup",
        }
    )
    validate = True
    df = pd.DataFrame(
        {
            "Wahlperiode": [0],
            "Sitzungnr": [0],
            "Abstimmnr": [0],
            "Fraktion/Gruppe": ["wup"],
            "Name": ["wup"],
            "Vorname": ["wup"],
            "Titel": [""],
            "ja": [1 if invalid_votes else 0],
            "nein": [1],
            "Enthaltung": [0],
            "ungültig": [0],
            "nichtabgegeben": [0],
            "Bezeichnung": ["wup"],
            "Bemerkung": [""],
            "date": [date],
            "title": [title],
            "sheet_name": [sheet_name],
        }
    )

    dfs = {i: df for i in range(n_dfs)} if n_dfs > 0 else None

    with (
        patch(
            "bundestag.data.transform.bundestag_sheets.file_size_is_zero",
            MagicMock(return_value=file_size_is_zero),
        ) as _file_size_is_zero,
        patch(
            "bundestag.data.transform.bundestag_sheets.read_excel",
            MagicMock(return_value=dfs),
        ) as _read_excel,
        patch(
            "bundestag.data.transform.bundestag_sheets.disambiguate_party",
            MagicMock(return_value=df),
        ) as _disambiguate_party,
        patch(
            "bundestag.data.transform.bundestag_sheets.handle_title_and_date",
            MagicMock(return_value=[title, date]),
        ) as _handle_title_and_date,
        patch("bundestag.schemas.SHEET.validate", MagicMock()) as _validate,
    ):
        try:
            # line to test
            res = transform_bs.get_sheet_df(sheet_file, file_title_maps, validate)
        except ValueError as ex:
            if len(dfs) > 1:
                pytest.xfail(f"ValueError raised because of multiple sheets: {ex}")

        _file_size_is_zero.assert_called_once_with(sheet_file)
        if file_size_is_zero:
            assert res is None
            _read_excel.assert_not_called()
        else:
            _read_excel.assert_called_once_with(sheet_file)
            if n_dfs == 0:
                assert res is None
                _disambiguate_party.assert_not_called()
            else:
                if file_title_maps is not None:
                    _handle_title_and_date.assert_called_once_with(
                        full_title, sheet_file
                    )
                assert isinstance(res, pd.DataFrame)
                _disambiguate_party.assert_called_once_with(df)
                if validate:
                    _validate.assert_called_once()


def test_get_sheet_df_with_actual_data():
    path = Path("tests/data_for_testing/20201126_3_xls-data.xlsx")
    file_title_maps = {
        "20201126_3_xls-data.xlsx": "26.11.2020: Übereinkommen über ein Einheitliches Patentgericht",
        "20201126_2_xls-data.xlsx": "26.11.2020: Europäische Bank für nachhaltige Entwicklung (Beschlussempfehlung)",
        "20201118_4_xls-data.xlsx": "18.11.2020: Corona-Maßnahmen (epidemische Lage), Antrag CDU/CSU, SPD",
        "20201118_1_xls-data.xlsx": "18.11.2020: Corona-Maßnahmen (epidemische Lage), Änderungsantrag FDP",
        "20200916_1_xls-data.xlsx": "16.09.2020: Mobilität der Zukunft (Beschlussempfehlung)",
    }

    # line to test
    df = transform_bs.get_sheet_df(path, file_title_maps=file_title_maps, validate=True)


@pytest.mark.parametrize(
    "full_title,sheet_file,exp_title,exp_date",
    [
        (
            "26.11.2020: Übereinkommen über ein Einheitliches Patentgericht",
            Path("some/file/bla.csv"),
            "Übereinkommen über ein Einheitliches Patentgericht",
            pd.to_datetime("2020-11-26"),
        ),
        (
            "Übereinkommen über ein Einheitliches Patentgericht",
            Path("some/file/2020-11-26_bla.csv"),
            "Übereinkommen über ein Einheitliches Patentgericht",
            pd.to_datetime("2020-11-26"),
        ),
        (
            "Übereinkommen über ein Einheitliches Patentgericht",
            Path("some/file/bla.csv"),
            "Übereinkommen über ein Einheitliches Patentgericht",
            None,
        ),
    ],
)
def test_handle_title_and_date(
    full_title: str,
    sheet_file: Path,
    exp_title: str,
    exp_date: pd.DatetimeTZDtype,
):
    # full_title = "26.11.2020: Übereinkommen über ein Einheitliches Patentgericht"
    # sheet_file = Path("some/file/2020-11-26_bla.csv")

    # line to test
    title, date = transform_bs.handle_title_and_date(full_title, sheet_file)
    assert title == exp_title
    if exp_date is None:
        assert date is None
    else:
        assert date == exp_date


def test_disambiguate_party():
    col = "Fraktion/Gruppe"
    known = list(transform_bs.PARTY_MAP.keys())
    unknown = ["wuppety"]
    df = pd.DataFrame({col: known + unknown})

    # line to test
    df2 = transform_bs.disambiguate_party(
        df.copy(), col=col, party_map=transform_bs.PARTY_MAP
    )

    assert df2[col].iloc[-1] == df[col].iloc[-1]
    assert not df2[col].iloc[:-1].equals(df[col].iloc[:-1])


@pytest.mark.parametrize("validate", [True, False])
def test_get_squished_dataframe(validate: bool):
    path = Path("tests/data_for_testing/20201126_3_xls-data.xlsx")
    file_title_maps = {
        "20201126_3_xls-data.xlsx": "26.11.2020: Übereinkommen über ein Einheitliches Patentgericht",
        "20201126_2_xls-data.xlsx": "26.11.2020: Europäische Bank für nachhaltige Entwicklung (Beschlussempfehlung)",
        "20201118_4_xls-data.xlsx": "18.11.2020: Corona-Maßnahmen (epidemische Lage), Antrag CDU/CSU, SPD",
        "20201118_1_xls-data.xlsx": "18.11.2020: Corona-Maßnahmen (epidemische Lage), Änderungsantrag FDP",
        "20200916_1_xls-data.xlsx": "16.09.2020: Mobilität der Zukunft (Beschlussempfehlung)",
    }

    df = transform_bs.get_sheet_df(
        path, file_title_maps=file_title_maps, validate=validate
    )

    # line to test
    df = transform_bs.get_squished_dataframe(df, validate=validate)


@pytest.mark.parametrize(
    "dtypes",
    [
        {"a": "int64", "b": "int64", "c": "object"},
        {"a": int, "b": int, "c": object},
        {"a": int, "b": int, "c": str},
    ],
    ids=["object", "object2", "str"],
)
def test_set_sheet_dtypes(dtypes: T.Dict[str, T.Any]):
    df = pd.DataFrame({"a": [1, 2, 3], "b": [1.0, 2.0, 3.0], "c": ["a", "b", "c"]})
    df_target = pd.DataFrame({"a": [1, 2, 3], "b": [1, 2, 3], "c": ["a", "b", "c"]})

    # line to test
    df2 = transform_bs.set_sheet_dtypes(df.copy(), dtypes)
    assert df2.dtypes.equals(df_target.dtypes)


def test_get_final_sheet_df():
    path = Path("tests/data_for_testing/20201126_3_xls-data.xlsx")
    file_title_maps = {
        "20201126_3_xls-data.xlsx": "26.11.2020: Übereinkommen über ein Einheitliches Patentgericht",
        "20201126_2_xls-data.xlsx": "26.11.2020: Europäische Bank für nachhaltige Entwicklung (Beschlussempfehlung)",
        "20201118_4_xls-data.xlsx": "18.11.2020: Corona-Maßnahmen (epidemische Lage), Antrag CDU/CSU, SPD",
        "20201118_1_xls-data.xlsx": "18.11.2020: Corona-Maßnahmen (epidemische Lage), Änderungsantrag FDP",
        "20200916_1_xls-data.xlsx": "16.09.2020: Mobilität der Zukunft (Beschlussempfehlung)",
    }

    df = transform_bs.get_sheet_df(
        path, file_title_maps=file_title_maps, validate=False
    )

    # lines to test
    df = transform_bs.get_squished_dataframe(df)
    df = transform_bs.set_sheet_dtypes(df)
    schemas.SHEET_FINAL.validate(df)


@pytest.mark.parametrize("one_fails", [True, False])
def test_get_multiple_sheets(one_fails: bool):
    sheet_files = [
        Path("tests/data_for_testing/20201126_3_xls-data.xlsx"),
        Path("tests/data_for_testing/20201126_2_xls-data.xlsx"),
    ]
    file_title_maps = None
    dfs = [pd.DataFrame({"a": [1]}), pd.DataFrame({"a": [2]})]
    if one_fails:
        dfs[0] = None
    fine_dfs = [df for df in dfs if df is not None]

    with (
        patch(
            "bundestag.data.transform.bundestag_sheets.get_sheet_df",
            side_effect=dfs,
        ) as _get_sheet_df,
        patch(
            "bundestag.data.transform.bundestag_sheets.get_squished_dataframe",
            side_effect=fine_dfs,
        ) as _get_squished_dataframe,
        patch(
            "bundestag.data.transform.bundestag_sheets.set_sheet_dtypes",
            side_effect=fine_dfs,
        ) as _set_sheet_types,
    ):
        # line to test
        df = transform_bs.get_multiple_sheets_df(
            sheet_files, file_title_maps=file_title_maps
        )

        assert _get_sheet_df.call_count == len(sheet_files)
        if one_fails:
            assert _get_squished_dataframe.call_count == 1
            assert _set_sheet_types.call_count == 1
        else:
            assert _get_squished_dataframe.call_count == 2
            assert _set_sheet_types.call_count == 2


@pytest.mark.parametrize(
    "dry,html_path_exists,sheet_path_exists,preprocessed_path_exists,html_path,sheet_path,preprocessed_path,validate",
    [
        (
            dry,
            html_path_exists,
            sheet_path_exists,
            preprocessed_path_exists,
            html_path,
            sheet_path,
            preprocessed_path,
            validate,
        )
        for dry in [True, False]
        for html_path_exists in [True, False]
        for sheet_path_exists in [True, False]
        for preprocessed_path_exists in [True, False]
        for html_path in [
            Path("raw/path"),
        ]
        for sheet_path in [
            Path("raw/path"),
        ]
        for preprocessed_path in [
            Path("preprocessed/path"),
        ]
        for validate in [True, False]
    ],
)
def test_run(
    dry: bool,
    html_path_exists: bool,
    sheet_path_exists: bool,
    preprocessed_path_exists: bool,
    html_path: Path,
    sheet_path: Path,
    preprocessed_path: Path,
    validate: bool,
):
    with (
        patch(
            "pathlib.Path.exists",
            MagicMock(
                side_effect=[
                    html_path_exists,
                    sheet_path_exists,
                    preprocessed_path_exists,
                ]
            ),
        ) as _exists,
        patch("bundestag.data.utils.ensure_path_exists", MagicMock()) as _ensure_exists,
        patch("pandas.DataFrame.to_parquet", MagicMock()) as _to_parquet,
        patch(
            "bundestag.data.utils.get_file_paths", MagicMock(return_value=[])
        ) as _get_file_paths,
        patch(
            "bundestag.data.download.bundestag_sheets.collect_sheet_uris",
            MagicMock(return_value=[]),
        ) as _collect_sheet_uris,
        patch(
            "bundestag.data.transform.bundestag_sheets.get_file2poll_maps",
            MagicMock(return_value={}),
        ) as _get_file2poll_maps,
        patch(
            "bundestag.data.transform.bundestag_sheets.get_multiple_sheets_df",
            MagicMock(return_value=pd.DataFrame({"a": [1]})),
        ) as _get_multiple_sheets_df,
    ):
        try:
            # line to test
            transform_bs.run(
                html_path=html_path,
                sheet_path=sheet_path,
                preprocessed_path=preprocessed_path,
                dry=dry,
                validate=validate,
            )
        except ValueError as ex:
            if not dry and (not html_path_exists or not sheet_path_exists):
                pytest.xfail("ValueError for missing path")
            else:
                raise ex

        if not dry:
            assert _exists.call_count == 3
        if not dry and not preprocessed_path_exists:
            _ensure_exists.assert_called_once_with(preprocessed_path)

        if not dry:
            _to_parquet.assert_has_calls(
                [call(preprocessed_path / "bundestag.de_votes.parquet")]
            )

        _get_file_paths.assert_has_calls(
            [
                call(html_path, pattern=data_utils.RE_HTM),
                call(sheet_path, pattern=data_utils.RE_FNAME),
            ]
        )

        _collect_sheet_uris.assert_called_once_with([])
        _get_file2poll_maps.assert_called_once_with([], sheet_path)
        _get_multiple_sheets_df.assert_called_once_with(
            [], file_title_maps={}, validate=validate
        )
