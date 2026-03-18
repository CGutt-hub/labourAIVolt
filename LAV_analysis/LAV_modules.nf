// LAV Modules — all using IOInterface from AnalysisToolbox
// Each alias maps a human-readable step name to the generic IOInterface process.

// Core framework processes
include { participant_discovery; finalize_participant; finalize_l2 } from '../../AnalysisToolbox/Python/utils/workflow_wrapper.nf'

// ── Readers ───────────────────────────────────────────────────────────────
// Fetches labour market data from World Bank API for one country
include { IOInterface as api_reader } from '../../AnalysisToolbox/Python/utils/workflow_wrapper.nf'

// ── Processors ────────────────────────────────────────────────────────────
// Pivots raw long-format data to wide time-series (not statistical normalization)
include { IOInterface as normalizing_processor } from '../../AnalysisToolbox/Python/utils/workflow_wrapper.nf'

// ── OLS Analyzer (L1 — per country) ──────────────────────────────────────
// Single step: time-series OLS for all indicators + displacement scores
// (Consolidates former displacement_analyzer + trend_analyzer)
// Produces: {pid}_ols.parquet, {pid}_ols_vis.parquet, {pid}_displacement_vis.parquet
include { IOInterface as ols_analyzer } from '../../AnalysisToolbox/Python/utils/workflow_wrapper.nf'

// ── Group-level (L2 — cross-country) ─────────────────────────────────────
// Synthesises all countries via AT group_analyzer + policy table
// Produces: LAV_volt_report.parquet, cross-country vis.parquets
include { IOInterface as volt_report_analyzer } from '../../AnalysisToolbox/Python/utils/workflow_wrapper.nf'
