/*
 * Copyright 2020 Spotify AB.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.flyte.examples;

import com.google.auto.service.AutoService;
import flyteidl.flink.Flink.FlinkJob;
import org.flyte.flytekit.SdkWorkflow;
import org.flyte.flytekit.SdkWorkflowBuilder;
import org.flyte.flytekit.flink.RunFlinkJob;

@AutoService(SdkWorkflow.class)
public class HelloFlinkWorkflow extends SdkWorkflow {

  @Override
  public void expand(SdkWorkflowBuilder builder) {
    FlinkJob job =
        FlinkJob.newBuilder()
            .setMainClass("com.spotify.heroic.importer.GCSAvroToBigqueryJob")
            .setJarFile("gs://esquilo-random/heroic-bigquery-importer.jar")
            .setImage("gcr.io/esquilo/flink:1.10.1-scala_2.12-gcs")
            .setServiceAccount("ff-dev-workload-sa")
            .addArgs("--runner=FlinkRunner")
            .addArgs("--tempLocation=gs://heroic-bigquery-importer-staging/temp")
            .addArgs("--input=gs://esquilo-datasets/heroic/gew/imported-metrics-avro/2020/08/31")
            .addArgs("--bqtable=esquilo:metrics_all.metrics_all_gew")
            .addArgs("--output=not_needed")
            .build();

    builder.apply("flink-example", RunFlinkJob.of(job));
  }
}
