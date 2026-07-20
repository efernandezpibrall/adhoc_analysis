from __future__ import annotations

import pandas as pd
import pytest
from openpyxl import load_workbook

import ea_export_flow_excel as report


@pytest.mark.parametrize("value", [True, 1.0, "1", 0, -1])
def test_resolve_ea_run_rejects_non_integer_and_nonpositive_run_ids(value):
    with pytest.raises(ValueError, match="positive integer"):
        report.resolve_ea_run(object(), value)


def test_country_matrix_preserves_columns_totals_and_month_fill():
    source = pd.DataFrame(
        {
            "month": ["2026-01-01", "2026-01-01", "2026-03-01"],
            "country_name": ["United States", "Unknown", "Qatar"],
            "total_mmtpa": [10.125, 2.0, 5.0],
        }
    )

    result = report.build_country_matrix(source)

    assert result.columns.tolist() == report.OUTPUT_COLUMNS
    assert result["Month"].dt.strftime("%Y-%m").tolist() == [
        "2026-01", "2026-02", "2026-03"
    ]
    assert result["US"].tolist() == [10.12, 0.0, 0.0]
    assert result["Rest of the World"].tolist() == [2.0, 0.0, 0.0]
    assert result["Qatar"].tolist() == [0.0, 0.0, 5.0]
    assert result["Total MMTPA"].tolist() == [12.12, 0.0, 5.0]


def test_excel_output_keeps_sheet_headers_and_number_formats(tmp_path):
    frame = pd.DataFrame(
        [[pd.Timestamp("2026-01-01"), 1.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0]],
        columns=report.OUTPUT_COLUMNS,
    )
    output = report.export_to_excel(frame, tmp_path / "ea.xlsx")
    workbook = load_workbook(output)
    sheet = workbook[report.SHEET_NAME]

    assert [cell.value for cell in sheet[1]] == report.OUTPUT_COLUMNS
    assert sheet.freeze_panes == "A2"
    assert sheet["A2"].number_format == "yyyy-mm"
    assert sheet["B2"].number_format == "0.00"
