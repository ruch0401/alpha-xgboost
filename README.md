# XGBoost

Compatibility POC for XGBoost.

# Commands

```bash
spark-submit --driver-java-options "-DNODE_TLS_REJECT_UNAUTHORIZED=0" --jars ../resources/spark-al-filter-core_external.jar alpha_train.py
```

```bash
spark-submit --driver-java-options "-DNODE_TLS_REJECT_UNAUTHORIZED=0" --jars ../resources/spark-al-filter-core_external.jar alpha_test.py
```