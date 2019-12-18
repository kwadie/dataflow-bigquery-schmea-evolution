#!/usr/bin/env bash

mvn compile exec:java \
-Dexec.mainClass=com.google.cloud.pso.pipeline.PubsubAvroToBigQueryDynamicDestinations \
-Dexec.args=" \
--subscription=projects/wadie-bq-sandbox/subscriptions/wadie-employess-pull \
--outputTableProject=wadie-bq-sandbox \
--outputTableDataset=schema_evolution_poc \
--project=wadie-bq-sandbox \
--streaming=true \
--runner=DataflowRunner"
