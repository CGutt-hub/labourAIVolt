#!/usr/bin/env nextflow
nextflow.enable.dsl=2

// =========================================
// LAV (Labour AI Volt) Nextflow Pipeline
// =========================================
// Analyses AI-driven labour displacement across key Volt EU countries.
//
// Requires: AnalysisToolbox cloned as a sibling directory
//   ../  (repo root)
//   ├── AnalysisToolbox/
//   └── labourAIVolt/
//       ├── LAV_analysis/    ← launch from here
//       ├── LAV_data/        ← per-country config inputs
//       └── LAV_results/     ← outputs (mirrors EV_results/)
//
// Run:
//   cd LAV_analysis
//   nextflow run LAV_pipeline.nf -c LAV_parameters.config

include {
    participant_discovery; finalize_participant; finalize_l2;
    api_reader;
    normalizing_processor;
    displacement_analyzer; trend_analyzer;
    volt_report_analyzer;
} from './LAV_modules.nf'

// ── WORKFLOW ──────────────────────────────────────────────────────────────
workflow {

    // ── L1: Per-country analysis ──────────────────────────────────────────

    // Step 1: Discover countries (LAV_001 … LAV_006) and create output folders
    //         Mirrors EV_pipeline: participant_discovery scans LAV_data/LAV_*/
    participant_discovery(
        params.input_dir,
        params.output_dir,
        params.participant_pattern
    )

    participant_context = participant_discovery.out.participant_context
    participant_id      = participant_context.map { it[0] }

    // Step 2: Map each participant ID to its JSON config file
    //         e.g. LAV_data/LAV_001/LAV_001_config.json
    config_files = participant_id.map { id ->
        "${workflow.launchDir}/${params.input_dir}/${id}/${id}_config.json"
    }

    // Step 3: Fetch labour market data from the World Bank public API
    raw_data = api_reader(
        params.python_exe,
        params.api_reader_script,
        config_files,
        ""
    )

    // Step 4: Normalize — pivot long→wide, sort, deduplicate
    normalized = normalizing_processor(
        params.python_exe,
        params.normalizing_processor_script,
        raw_data,
        ""
    )

    // Step 5a: AI displacement analysis
    //          Computes sector displacement scores cross-referenced with
    //          Frey & Osborne (2013) automation-risk estimates
    displacement = displacement_analyzer(
        params.python_exe,
        params.displacement_analyzer_script,
        normalized,
        ""
    )

    // Step 5b: Time-series trend analysis
    //          OLS slope + p-value + R² for every labour-market indicator
    trends = trend_analyzer(
        params.python_exe,
        params.trend_analyzer_script,
        normalized,
        ""
    )

    // Per-country finalization: writes log.parquet, updates HTML archive, git sync
    // (mirrors EV_pipeline terminal_outputs list)
    def terminal_outputs = [displacement, trends]
    finalize_participant(terminal_outputs, participant_context)

    // ── L2: Cross-country group analysis ──────────────────────────────────

    // Collect all per-country displacement and trend files,
    // then synthesise cross-country rankings and Volt policy metrics
    all_group_inputs = displacement.mix(trends).collect()

    volt_report = volt_report_analyzer(
        params.python_exe,
        params.volt_report_analyzer_script,
        all_group_inputs,
        "group_log"
    )

    // Finalize group-level folder (LAV_l2): commit results + push
    finalize_l2(volt_report)
}
