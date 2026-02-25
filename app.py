from __future__ import annotations

import json
import tempfile
from pathlib import Path

import streamlit as st

from pb_ingestor.pipeline import run_conversion, run_enrichment

st.set_page_config(page_title="PB Ingestor", layout="wide")
st.title("Pricebook Ingestor")
st.caption("Upload a distributor file, convert into template output, and optionally run enrichment.")

with st.sidebar:
    st.header("Settings")
    template_type = st.selectbox("Template Type", ["single_part", "bundle", "supplier_loader"], index=1)
    markup_profile = st.text_input("Markup profile JSON", value="config/markup/default_global_tiered_markup.json")
    domains_config = st.text_input("Domains config JSON", value="config/enrichment/manufacturer_domains.json")
    labor_cost_default = st.number_input("Labor Cost Default", value=0.0, step=1.0)
    labor_rate_default = st.number_input("Labor Rate Default", value=125.0, step=1.0)

uploaded = st.file_uploader("Upload distributor workbook", type=["xlsx", "xlsm", "xls", "csv", "txt"])

if "last_outputs" not in st.session_state:
    st.session_state.last_outputs = {}

if uploaded is not None:
    with tempfile.TemporaryDirectory() as tmpd:
        tmp = Path(tmpd)
        source_path = tmp / uploaded.name
        source_path.write_bytes(uploaded.getvalue())

        col1, col2 = st.columns(2)
        with col1:
            if st.button("Run Convert", type="primary", use_container_width=True):
                out_dir = tmp / "out"
                out_dir.mkdir(parents=True, exist_ok=True)
                try:
                    result = run_conversion(
                        source=str(source_path),
                        template_type=template_type,
                        markup_profile_path=markup_profile,
                        output_csv=str(out_dir / "converted.csv"),
                        output_workbook=str(out_dir / "converted.xlsx"),
                        qa_json=str(out_dir / "qa.json"),
                        manual_review_csv=str(out_dir / "manual_review.csv"),
                        labor_cost_default=labor_cost_default if labor_cost_default > 0 else None,
                        labor_rate_default=labor_rate_default if labor_rate_default > 0 else None,
                    )
                except Exception as exc:
                    st.error(f"Conversion failed: {exc}")
                else:
                    st.success("Conversion completed")
                    st.json(result["summary"])
                    st.session_state.last_outputs = {
                        "converted_csv": Path(result["output_csv"]).read_bytes(),
                        "converted_xlsx": Path(result["output_workbook"]).read_bytes(),
                        "qa_json": Path(result["qa_json"]).read_bytes(),
                        "manual_review_csv": Path(result["manual_review_csv"]).read_bytes(),
                    }

        with col2:
            if st.button("Run Enrichment", use_container_width=True):
                if not st.session_state.last_outputs.get("converted_csv"):
                    st.warning("Run convert first so there is a CSV to enrich.")
                else:
                    enrich_in = tmp / "converted_for_enrich.csv"
                    enrich_in.write_bytes(st.session_state.last_outputs["converted_csv"])
                    try:
                        summary = run_enrichment(
                            input_csv=str(enrich_in),
                            output_csv=str(tmp / "enriched.csv"),
                            qa_json=str(tmp / "enrich_qa.json"),
                            domains_config=domains_config,
                            sleep_ms=50,
                        )
                    except Exception as exc:
                        st.error(f"Enrichment failed: {exc}")
                    else:
                        st.success("Enrichment completed")
                        st.json(summary)
                        st.session_state.last_outputs["enriched_csv"] = (tmp / "enriched.csv").read_bytes()
                        st.session_state.last_outputs["enrichment_qa_json"] = (tmp / "enrich_qa.json").read_bytes()

if st.session_state.last_outputs:
    st.subheader("Downloads")
    dl1, dl2, dl3 = st.columns(3)
    with dl1:
        if "converted_xlsx" in st.session_state.last_outputs:
            st.download_button("Download Converted XLSX", st.session_state.last_outputs["converted_xlsx"], file_name="converted.xlsx")
        if "converted_csv" in st.session_state.last_outputs:
            st.download_button("Download Converted CSV", st.session_state.last_outputs["converted_csv"], file_name="converted.csv")
    with dl2:
        if "manual_review_csv" in st.session_state.last_outputs:
            st.download_button("Download Manual Review CSV", st.session_state.last_outputs["manual_review_csv"], file_name="manual_review.csv")
        if "qa_json" in st.session_state.last_outputs:
            st.download_button("Download QA JSON", st.session_state.last_outputs["qa_json"], file_name="qa.json")
    with dl3:
        if "enriched_csv" in st.session_state.last_outputs:
            st.download_button("Download Enriched CSV", st.session_state.last_outputs["enriched_csv"], file_name="enriched.csv")
        if "enrichment_qa_json" in st.session_state.last_outputs:
            st.download_button("Download Enrichment QA JSON", st.session_state.last_outputs["enrichment_qa_json"], file_name="enrichment_qa.json")

st.subheader("Quick start")
st.code(
    "python -m pip install -e .\n"
    "streamlit run app.py --server.port 8501",
    language="bash",
)

with st.expander("Debug info"):
    st.write("Markup profile path", markup_profile)
    st.write("Domains config path", domains_config)
    st.write("Session keys", list(st.session_state.keys()))
