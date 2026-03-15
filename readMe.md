alpha score architecture
alpha_system/
├─ data_ingestion/
│  ├─ daily_loader
│  ├─ book_loader
│  └─ tick_loader
│
├─ data_processing/
│  ├─ cleaning
│  ├─ alignment
│  ├─ normalization
│  ├─ session_handler
│  └─ quality_checks
│
├─ feature_engine/
│  ├─ daily_features
│  ├─ tick_features
│  ├─ book_features
│  ├─ hybrid_features
│  └─ feature_registry
│
├─ alpha_engine/
│  ├─ rule_based
│  ├─ statistical
│  ├─ ml_models
│  └─ signal_aggregator
│
├─ research/
│  ├─ labeling
│  ├─ metrics
│  ├─ robustness_tests
│  ├─ walk_forward
│  └─ feature_selection
│
├─ portfolio/
│  ├─ ranking
│  ├─ sizing
│  └─ allocation
│
├─ risk/
│  ├─ pretrade_checks
│  ├─ exposure_limits
│  ├─ market_risk
│  └─ kill_switch
│
├─ backtest/
│  ├─ fast_backtest
│  ├─ event_backtest
│  └─ execution_simulator
│
├─ serving/
│  ├─ online_feature_calc
│  ├─ signal_service
│  └─ monitoring
│
└─ common/
   ├─ config
   ├─ schema
   ├─ utils
   └─ logging