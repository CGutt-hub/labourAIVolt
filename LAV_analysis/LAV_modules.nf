// LAV Modules — all using IOInterface from AnalysisToolbox
// Each alias maps a human-readable step name to the generic IOInterface process.

// Core framework processes
include { participant_discovery; finalize_participant; finalize_l2 } from '../../AnalysisToolbox/Python/utils/workflow_wrapper.nf'

// ── Readers ──────────────────────────────────────────────────────────────
// Fetches labour market data from World Bank API for one country
include { IOInterface as api_reader } from '../../AnalysisToolbox/Python/utils/workflow_wrapper.nf'

// ── Processors ───────────────────────────────────────────────────────────
// Cleans and pivots raw long-format data to wide time-series
include { IOInterface as normalizing_processor } from '../../AnalysisToolbox/Python/utils/workflow_wrapper.nf'

// ── Analyzers (L1 — per country) ─────────────────────────────────────────
// Computes AI displacement scores by sector (Frey & Osborne cross-reference)
include { IOInterface as displacement_analyzer } from '../../AnalysisToolbox/Python/utils/workflow_wrapper.nf'

// Fits linear time-series trends to every labour indicator
include { IOInterface as trend_analyzer } from '../../AnalysisToolbox/Python/utils/workflow_wrapper.nf'

// ── File finders (extract specific outputs from multi-file processes) ─────
include { IOInterface as displacement_file_finder } from '../../AnalysisToolbox/Python/utils/workflow_wrapper.nf'
include { IOInterface as trend_file_finder        } from '../../AnalysisToolbox/Python/utils/workflow_wrapper.nf'

// ── Group-level (L2 — cross-country) ─────────────────────────────────────
// Synthesises all countries: displacement ranking, trend comparison, Volt policy metrics
include { IOInterface as volt_report_analyzer } from '../../AnalysisToolbox/Python/utils/workflow_wrapper.nf'
