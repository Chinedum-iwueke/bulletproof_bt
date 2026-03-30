# Engine Output Contract Audit (reference rich trade artifact)

- Generated at (UTC): 2026-03-30T09:22:52.492921+00:00
- Entrypoint: bt.saas.service.run_analysis_from_parsed_artifact(parsed_artifact, config)
- Reference CSV: examples/reference_artifacts/trades_rich_reference.csv
- Output directory: debug/audit/engine_contract_reference

## Top-level keys
- capability_profile, diagnostics, raw_payload, run_context, warnings

## Diagnostics
### overview
- status: available
- summary_metric_keys: confidence, expectancy, overfitting_risk, payoff_ratio, posture, profit_factor, realized_max_drawdown_pct, robustness_score, ruin_probability, trade_count, win_rate, worst_mc_drawdown_pct
- figure_count: 1
- figure_types: line_series
  - figure id=equity_curve type=line_series populated=True payload_keys=x, series

### distribution
- status: available
- summary_metric_keys: avg_duration_minutes, expectancy, gross_loss, gross_loss_abs, gross_profit, kurtosis, mean_duration, mean_return, median_duration, median_duration_minutes, median_return, payoff_ratio, percentile_10, percentile_90, profit_factor, return_std, skewness, trade_count, win_rate
- figure_count: 5
- figure_types: bar_groups, histogram, scatter
  - figure id=trade_return_histogram type=histogram populated=True payload_keys=bins, metadata
  - figure id=win_loss_distribution type=bar_groups populated=True payload_keys=groups
  - figure id=mae_mfe_scatter type=scatter populated=True payload_keys=points
  - figure id=duration_histogram type=histogram populated=True payload_keys=bins
  - figure id=r_multiple_histogram type=histogram populated=True payload_keys=bins

### monte_carlo
- status: available
- summary_metric_keys: drawdown_p95_pct, expected_drawdown, median_drawdown, median_drawdown_pct, p5_drawdown, p95_drawdown, p_ruin, path_dispersion_terminal_equity_p10, path_dispersion_terminal_equity_p90, probability_of_ruin, recovery_median_trades, recovery_success_rate, ruin_threshold_equity, worst_drawdown, worst_simulated_drawdown_pct
- figure_count: 2
- figure_types: fan_chart, histogram
  - figure id=equity_fan_chart type=fan_chart populated=True payload_keys=x, bands
  - figure id=drawdown_histogram type=histogram populated=True payload_keys=bins

### stability
- status: limited
- summary_metric_keys: peak_fragility, plateau_ratio, stability_score
- figure_count: 0
- figure_types: <none>

### execution
- status: available
- summary_metric_keys: baseline_ev_net, baseline_expectancy, break_even_cost_multiplier, break_even_cost_threshold_bps, edge_decay_abs, edge_decay_pct, execution_resilience_score, stressed_expectancy, stressed_scenario
- figure_count: 1
- figure_types: line_series
  - figure id=execution_expectancy_decay type=line_series populated=True payload_keys=x, series

### regimes
- status: unavailable
- summary_metric_keys: <none>
- figure_count: 0
- figure_types: <none>

### ruin
- status: available
- summary_metric_keys: expected_stress_drawdown, max_tolerable_risk_per_trade, minimum_survivable_capital, probability_of_ruin, survival_probability
- figure_count: 2
- figure_types: line_series
  - figure id=ruin_probability_curve type=line_series populated=True payload_keys=x, series
  - figure id=risk_per_trade_sensitivity type=line_series populated=True payload_keys=x, series

### report
- status: available
- summary_metric_keys: available_diagnostic_count, robustness_score, trade_count
- figure_count: 0
- figure_types: <none>

## Report payload shape
{
  "assumptions": [
    "str"
  ],
  "available": "bool",
  "figures": [],
  "interpretation": {
    "cautions": [
      "str"
    ],
    "positives": [],
    "summary": "str"
  },
  "limitations": [
    "str"
  ],
  "limited": "bool",
  "metadata": {
    "available_diagnostics": {
      "distribution": "bool",
      "execution": "bool",
      "monte_carlo": "bool",
      "overview": "bool",
      "regimes": "bool",
      "report": "bool",
      "ruin": "bool",
      "stability": "bool"
    },
    "compatibility": {
      "canonical_report_path": "str",
      "deprecated_aliases_removed": [
        "str"
      ]
    },
    "export_sections": [
      "str"
    ]
  },
  "payload": {
    "assumptions": [
      "str"
    ],
    "figures": [],
    "interpretation": {
      "cautions": [
        "str"
      ],
      "positives": [],
      "summary": "str"
    },
    "metadata": {
      "available_diagnostics": {
        "distribution": "bool",
        "execution": "bool",
        "monte_carlo": "bool",
        "overview": "bool",
        "regimes": "bool",
        "report": "bool",
        "ruin": "bool",
        "stability": "bool"
      },
      "compatibility": {
        "canonical_report_path": "str",
        "deprecated_aliases_removed": [
          "str"
        ]
      },
      "export_sections": [
        "str"
      ]
    },
    "recommendations": [
      "str"
    ],
    "report": {
      "confidence_level": {
        "level": "str",
        "summary": "str"
      },
      "deployment_guidance": {
        "deploy_now": "bool",
        "do_not_use_for": [
          "str"
        ],
        "narrative": "str",
        "recommended_scope": "str",
        "required_conditions_before_deploy": [
          "str"
        ]
      },
      "diagnostics_summary": {
        "distribution": {
          "confidence_impact": "str",
          "status": "str",
          "takeaway": "str"
        },
        "execution": {
          "confidence_impact": "str",
          "status": "str",
          "takeaway": "str"
        },
        "monte_carlo": {
          "confidence_impact": "str",
          "status": "str",
          "takeaway": "str"
        },
        "overview": {
          "confidence_impact": "str",
          "status": "str",
          "takeaway": "str"
        },
        "regimes": {
          "confidence_impact": "str",
          "status": "str",
          "takeaway": "str"
        },
        "report": {
          "confidence_impact": "str",
          "status": "str",
          "takeaway": "str"
        },
        "ruin": {
          "confidence_impact": "str",
          "status": "str",
          "takeaway": "str"
        },
        "stability": {
          "confidence_impact": "str",
          "status": "str",
          "takeaway": "str"
        }
      },
      "executive_summary": {
        "operational_implications": "str",
        "summary": "str",
        "what_matters_now": [
          "str"
        ]
      },
      "executive_verdict": {
        "headline": "str",
        "status": "str",
        "summary": "str"
      },
      "key_metrics_snapshot": {
        "edge_decay_pct": "float",
        "expectancy": "float",
        "probability_of_ruin": "float",
        "robustness_score": "float",
        "win_rate": "float",
        "worst_simulated_drawdown_pct": "float"
      },
      "limitations": [
        "str"
      ],
      "metadata": {
        "analysis_date": "str",
        "analysis_id": "str",
        "artifact_label": "str",
        "available_diagnostics": {
          "distribution": "bool",
          "execution": "bool",
          "monte_carlo": "bool",
          "overview": "bool",
          "regimes": "bool",
          "report": "bool",
          "ruin": "bool",
          "stability": "bool"
        },
        "export_readiness": {
          "audit_share_structured": "bool",
          "pdf_ready_core_sections": "bool",
          "screen_rendering_ready": "bool"
        },
        "report_scope": "str"
      },
      "methodology": {
        "artifact_richness": "str",
        "engine": "str",
        "ingestion_source": "str",
        "modeling_assumptions": [
          "str"
        ],
        "monte_carlo": {
          "enabled": "bool",
          "ruin_drawdown_levels": [],
          "seed": "int",
          "simulations": "int"
        },
        "parser_notes": [],
        "runtime_seam": "str"
      },
      "recommendations": [
        "str"
      ],
      "report_figures": [
        {
          "figure_key": "str",
          "section": "str",
          "title": "str"
        }
      ]
    },
    "summary_metrics": {
      "available_diagnostic_count": "int",
      "robustness_score": "float",
      "trade_count": "int"
    },
    "warnings": []
  },
  "reason_unavailable": "NoneType",
  "recommendations": [
    "str"
  ],
  "report": {
    "confidence_level": {
      "level": "str",
      "summary": "str"
    },
    "deployment_guidance": {
      "deploy_now": "bool",
      "do_not_use_for": [
        "str"
      ],
      "narrative": "str",
      "recommended_scope": "str",
      "required_conditions_before_deploy": [
        "str"
      ]
    },
    "diagnostics_summary": {
      "distribution": {
        "confidence_impact": "str",
        "status": "str",
        "takeaway": "str"
      },
      "execution": {
        "confidence_impact": "str",
        "status": "str",
        "takeaway": "str"
      },
      "monte_carlo": {
        "confidence_impact": "str",
        "status": "str",
        "takeaway": "str"
      },
      "overview": {
        "confidence_impact": "str",
        "status": "str",
        "takeaway": "str"
      },
      "regimes": {
        "confidence_impact": "str",
        "status": "str",
        "takeaway": "str"
      },
      "report": {
        "confidence_impact": "str",
        "status": "str",
        "takeaway": "str"
      },
      "ruin": {
        "confidence_impact": "str",
        "status": "str",
        "takeaway": "str"
      },
      "stability": {
        "confidence_impact": "str",
        "status": "str",
        "takeaway": "str"
      }
    },
    "executive_summary": {
      "operational_implications": "str",
      "summary": "str",
      "what_matters_now": [
        "str"
      ]
    },
    "executive_verdict": {
      "headline": "str",
      "status": "str",
      "summary": "str"
    },
    "key_metrics_snapshot": {
      "edge_decay_pct": "float",
      "expectancy": "float",
      "probability_of_ruin": "float",
      "robustness_score": "float",
      "win_rate": "float",
      "worst_simulated_drawdown_pct": "float"
    },
    "limitations": [
      "str"
    ],
    "metadata": {
      "analysis_date": "str",
      "analysis_id": "str",
      "artifact_label": "str",
      "available_diagnostics": {
        "distribution": "bool",
        "execution": "bool",
        "monte_carlo": "bool",
        "overview": "bool",
        "regimes": "bool",
        "report": "bool",
        "ruin": "bool",
        "stability": "bool"
      },
      "export_readiness": {
        "audit_share_structured": "bool",
        "pdf_ready_core_sections": "bool",
        "screen_rendering_ready": "bool"
      },
      "report_scope": "str"
    },
    "methodology": {
      "artifact_richness": "str",
      "engine": "str",
      "ingestion_source": "str",
      "modeling_assumptions": [
        "str"
      ],
      "monte_carlo": {
        "enabled": "bool",
        "ruin_drawdown_levels": [],
        "seed": "int",
        "simulations": "int"
      },
      "parser_notes": [],
      "runtime_seam": "str"
    },
    "recommendations": [
      "str"
    ],
    "report_figures": [
      {
        "figure_key": "str",
        "section": "str",
        "title": "str"
      }
    ]
  },
  "status": "str",
  "summary_metrics": {
    "available_diagnostic_count": "int",
    "robustness_score": "float",
    "trade_count": "int"
  },
  "warnings": []
}

## Benchmark-related fields detected
- capability_profile.artifact_capabilities.has_benchmark_context
- diagnostics.overview.benchmark_comparison
- diagnostics.overview.benchmark_comparison.metadata.benchmark_frequency
- diagnostics.overview.benchmark_comparison.metadata.benchmark_id
- diagnostics.overview.benchmark_comparison.metadata.benchmark_source
- diagnostics.overview.metadata.completeness_flags.benchmark_present
- diagnostics.overview.metadata.figure_provenance.benchmark_overlay
- diagnostics.overview.payload.benchmark_comparison
- diagnostics.overview.payload.benchmark_comparison.metadata.benchmark_frequency
- diagnostics.overview.payload.benchmark_comparison.metadata.benchmark_id
- diagnostics.overview.payload.benchmark_comparison.metadata.benchmark_source
- diagnostics.overview.payload.metadata.completeness_flags.benchmark_present
- diagnostics.overview.payload.metadata.figure_provenance.benchmark_overlay
- diagnostics.overview.payload.strategy.benchmark_present
- diagnostics.overview.strategy.benchmark_present
- raw_payload.artifact_capabilities.has_benchmark_context
- raw_payload.overview.benchmark_comparison
- raw_payload.overview.benchmark_comparison.metadata.benchmark_frequency
- raw_payload.overview.benchmark_comparison.metadata.benchmark_id
- raw_payload.overview.benchmark_comparison.metadata.benchmark_source
- raw_payload.overview.metadata.completeness_flags.benchmark_present
- raw_payload.overview.metadata.figure_provenance.benchmark_overlay
- raw_payload.overview.strategy.benchmark_present
- run_context.benchmark_present

## Placeholder / empty figure payloads
{}

## Raw figure key samples
{
  "distribution": [
    {
      "id": "trade_return_histogram",
      "keys": [
        "bins",
        "id",
        "metadata",
        "title",
        "type",
        "x_label",
        "y_label"
      ],
      "type": "histogram"
    },
    {
      "id": "win_loss_distribution",
      "keys": [
        "groups",
        "id",
        "title",
        "type",
        "x_label",
        "y_label"
      ],
      "type": "bar_groups"
    },
    {
      "id": "mae_mfe_scatter",
      "keys": [
        "id",
        "points",
        "title",
        "type",
        "x_label",
        "y_label"
      ],
      "type": "scatter"
    },
    {
      "id": "duration_histogram",
      "keys": [
        "bins",
        "id",
        "title",
        "type",
        "x_label",
        "y_label"
      ],
      "type": "histogram"
    },
    {
      "id": "r_multiple_histogram",
      "keys": [
        "bins",
        "id",
        "title",
        "type",
        "x_label",
        "y_label"
      ],
      "type": "histogram"
    }
  ],
  "execution": [
    {
      "id": "execution_expectancy_decay",
      "keys": [
        "id",
        "series",
        "title",
        "type",
        "x",
        "x_label",
        "y_label"
      ],
      "type": "line_series"
    }
  ],
  "monte_carlo": [
    {
      "id": "equity_fan_chart",
      "keys": [
        "bands",
        "id",
        "title",
        "type",
        "x",
        "x_label",
        "y_label"
      ],
      "type": "fan_chart"
    },
    {
      "id": "drawdown_histogram",
      "keys": [
        "bins",
        "id",
        "title",
        "type",
        "x_label",
        "y_label"
      ],
      "type": "histogram"
    }
  ],
  "overview": [
    {
      "id": "equity_curve",
      "keys": [
        "id",
        "series",
        "title",
        "type",
        "x",
        "x_label",
        "y_label"
      ],
      "type": "line_series"
    }
  ],
  "ruin": [
    {
      "id": "ruin_probability_curve",
      "keys": [
        "id",
        "series",
        "title",
        "type",
        "x",
        "x_label",
        "y_label"
      ],
      "type": "line_series"
    },
    {
      "id": "risk_per_trade_sensitivity",
      "keys": [
        "id",
        "series",
        "title",
        "type",
        "x",
        "x_label",
        "y_label"
      ],
      "type": "line_series"
    }
  ]
}
