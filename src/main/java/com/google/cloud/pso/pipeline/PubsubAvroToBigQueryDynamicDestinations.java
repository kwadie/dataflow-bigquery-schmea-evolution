/*
 * Copyright (C) 2018 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.cloud.pso.pipeline;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.pso.bigquery.TableRowWithSchema;
import com.google.cloud.pso.bigquery.TableRowWithSchemaCoder;
import com.google.cloud.pso.dofn.PubsubAvroToTableRowFn;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.Coder.Context;
import org.apache.beam.sdk.io.gcp.bigquery.*;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation.Required;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.AfterProcessingTime;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.Repeatedly;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.beam.sdk.values.ValueInSingleWindow;
import org.joda.time.Duration;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

/**
 * The {@link PubsubAvroToBigQueryDynamicDestinations} is a streaming pipeline which dynamically routes
 * messages to their output location using an attribute within the Pub/Sub message header. New tables
 * with schema will be generated if needed based on the PubSub Avro schema
 */
public class PubsubAvroToBigQueryDynamicDestinations {

    /**
     * The {@link Options} class provides the custom execution options passed by the executor at the
     * command-line.
     */
    public interface Options extends PipelineOptions {

        @Description("The Pub/Sub subscription to read messages from")
        @Required
        String getSubscription();

        void setSubscription(String value);


        @Description(
                "The name of the attribute which will contain the table project to route the message to.")
        @Required
        String getOutputTableProject();

        void setOutputTableProject(String value);

        @Description(
                "The name of the attribute which will contain the table dataset to route the message to.")
        @Required
        String getOutputTableDataset();

        void setOutputTableDataset(String value);
    }


    public static void main(String[] args) {
        Options options = PipelineOptionsFactory.fromArgs(args).as(Options.class);

        run(options);
    }

    /**
     * Runs the pipeline to completion with the specified options. This method does not wait until the
     * pipeline is finished before returning. Invoke {@code result.waitUntilFinish()} on the result
     * object to block until the pipeline is finished running if blocking programmatic execution is
     * required.
     *
     * @param options The execution options.
     * @return The pipeline result.
     */
    public static PipelineResult run(Options options) {

        Pipeline pipeline = Pipeline.create(options);

        // TableRowWithSchema must be registered to the pipeline for de/serialization
        TableRowWithSchemaCoder.registerCoder(pipeline);

        // Retrieve non-serializable parameters
        String outputTableProject = options.getOutputTableProject();
        String outputTableDataset = options.getOutputTableDataset();

        /*
        Steps:
        1. Read payload from PubSub
        2. Parse the Avro GenericRecords payload and convert to TableRowWithSchema
        3. Save to BigQuery using Dynamic Destinations and create new tables if needed
         */


        pipeline
                .apply(
                        "ReadAvroMessages",
                        PubsubIO
                                .readMessagesWithAttributes()
                                .fromSubscription(options.getSubscription()))
                .apply("PubsubAvroToTableRowFn",
                        ParDo
                                .of(new PubsubAvroToTableRowFn())
                                .withOutputTags(
                                        PubsubAvroToTableRowFn.MAIN_OUT,
                                        TupleTagList.of(PubsubAvroToTableRowFn.DEADLETTER_OUT)))
                .get(PubsubAvroToTableRowFn.MAIN_OUT)
                .apply(BigQueryIO.<TableRowWithSchema>write()
                        .to(new DynamicDestinations<TableRowWithSchema, TableRowWithSchema>() {

                            public TableRowWithSchema getDestination(ValueInSingleWindow<TableRowWithSchema> element) {

                                return element.getValue();
                            }

                            public TableDestination getTable(TableRowWithSchema row) {

                                String tableSpec = String.format(
                                        "%s:%s.%s",
                                        outputTableProject,
                                        outputTableDataset,
                                        row.getTableName());

                                return new TableDestination(tableSpec, null);
                            }

                            public TableSchema getSchema(TableRowWithSchema row) {

                                return row.getTableSchema();
                            }
                        })
                        .withFormatFunction(TableRowWithSchema::getTableRow)
                        .withCreateDisposition(CreateDisposition.CREATE_IF_NEEDED)
                        .withWriteDisposition(WriteDisposition.WRITE_APPEND));

        ;

        // TODO: handle DEADLETTER_OUT

        return pipeline.run();
    }
}
