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
//       ├── LAV_analysis/    <- launch from here
//       ├── LAV_data/        <- per-country config inputs (optional)
//       └── LAV_results/     <- outputs
//
// Run:
//   cd LAV_analysis
//   nextflow run LAV_pipeline.nf -c LAV_parameters.config

include {
    participant_discovery; finalize_participant; finalize_l2;
    api_reader;
    normalizing_processor;
    ols_analyzer;
    volt_report_analyzer;
} from './LAV_modules.nf'

// -- WORKFLOW -----------------------------------------------------------------
workflow {

    // -- L1: Per-country analysis ---------------------------------------------

    // Step 1: Discover countries (LAV_001 ... LAV_006) and create output folders
    participant_discovery(
        params.input_dir,
        params.output_dir,
        params.participant_pattern
    )

    participant_context = participant_discovery.out.participant_context
    participant_id      = participant_context.map { it[0] }

    // Step 2: Map each participant ID to its JSON config file
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

    // Step 4: Pivot long->wide, sort, deduplicate
    //         (AT pivot_processor.py will replace this once available)
    normalized = normalizing_processor(
        params.python_exe,
        params.normalizing_processor_script,
        raw_data,
        ""
    )

    // Step 5: OLS time-series analysis — trends + displacement scores in one pass
    //         Produces: {pid}_ols.parquet, {pid}_ols_vis.parquet,
    //                   {pid}_displacement_vis.parquet,
    //                   {pid}_sector_employment_vis.parquet,
    //                   {pid}_unemployment_vis.parquet
    //         (AT timeseries_ols_processor.py will replace the OLS loops
    //          once added to the toolbox; weighting stays in LAV)
    ols_results = ols_analyzer(
        params.python_exe,
        params.ols_analyzer_script,
        normalized,
        ""
    )

    // Per-country finalization: writes log.parquet, updates HTML archive
    finalize_participant([ols_results], participant_context)

    // -- L2: Cross-country group analysis -------------------------------------

    // Collect all per-country OLS files, then synthesise cross-country
    // rankings and Volt policy metrics using AT group_analyzer + policy table
    all_ols = ols_results.collect()

    volt_report = volt_report_analyzer(
        params.python_exe,
        params.volt_report_analyzer_script,
        all_ols,
        "group_log"
    )

    // Finalize group-level folder (LAV_l2): commit results + push
    finalize_l2(volt_report)
}
