# XGBoost

Compatibility POC for XGBoost.

# Commands

## For xgboost

```bash
spark-submit --driver-java-options "-DNODE_TLS_REJECT_UNAUTHORIZED=0" --jars ../resources/spark-al-filter-core_external.jar alpha_train.py
```

```bash
spark-submit --driver-java-options "-DNODE_TLS_REJECT_UNAUTHORIZED=0" --jars ../resources/spark-al-filter-core_external.jar alpha_test.py
```

## For performance testing

```bash
spark-submit --driver-java-options "-DNODE_TLS_REJECT_UNAUTHORIZED=0" --jars ../resources/spark-al-filter-core_external.jar performance.py [is_standard_object | is_standard_with_external_id | is_bitemporal_object | is_complete_snapshot_object | is_incremental_snapshot_object | is_transactional_object )]
```
