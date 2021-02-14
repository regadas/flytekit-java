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
package org.flyte.flytekit.flink;

import flyteidl.flink.Flink.FlinkJob;
import java.util.Map;

import com.google.errorprone.annotations.Var;

import org.flyte.flytekit.SdkBindingData;
import org.flyte.flytekit.SdkRemoteTask;
import org.flyte.flytekit.SdkTransform;
import org.flyte.flytekit.SdkTypes;

public final class RunFlinkJob {

  private RunFlinkJob() {}

  public static SdkTransform of() {
    return SdkRemoteTask.create(
        "production",
        "flyte-warehouse",
        "org.flyte.flytekit.flink.RunFlinkJob",
        MessageSdkType.of(FlinkJob.class),
        SdkTypes.nulls());
  }

  public static SdkTransform of(FlinkJob flinkJob) {
    Map<String, SdkBindingData> bindingData = ProtobufUtil.bindingData(flinkJob);
    @Var SdkTransform transform = of();

    for (Map.Entry<String, SdkBindingData> entry : bindingData.entrySet()) {
      transform = transform.withInput(entry.getKey(), entry.getValue());
    }

    return transform;
  }
}
