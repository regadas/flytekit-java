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

import com.google.auto.service.AutoService;
import com.google.errorprone.annotations.Var;
import flyteidl.flink.Flink.FlinkJob;
import java.util.List;
import java.util.Map;
import org.flyte.flytekit.SdkBindingData;
import org.flyte.flytekit.SdkNode;
import org.flyte.flytekit.SdkRemoteTask;
import org.flyte.flytekit.SdkRunnableTask;
import org.flyte.flytekit.SdkTransform;
import org.flyte.flytekit.SdkTypes;
import org.flyte.flytekit.SdkWorkflowBuilder;

@AutoService(SdkRunnableTask.class)
public final class RunFlinkJob extends SdkTransform {
  // public RunFlinkJob() {
  //   super(MessageSdkType.of(FlinkJob.class), SdkTypes.nulls());
  // }

  public static SdkTransform of(String project, String domain) {
    return SdkRemoteTask.create(
        domain,
        project,
        "org.flyte.flytekit.flink.RunFlinkJob",
        MessageSdkType.of(FlinkJob.class),
        SdkTypes.nulls());
  }

  public static SdkTransform of(String project, String domain, FlinkJob flinkJob) {
    Map<String, SdkBindingData> bindingData = ProtobufUtil.bindingData(flinkJob);
    @Var SdkTransform transform = of(project, domain);

    for (Map.Entry<String, SdkBindingData> entry : bindingData.entrySet()) {
      transform = transform.withInput(entry.getKey(), entry.getValue());
    }

    return transform;
  }

  public static SdkTransform of(FlinkJob flinkJob) {
    Map<String, SdkBindingData> bindingData = ProtobufUtil.bindingData(flinkJob);
    @Var SdkTransform transform = new RunFlinkJob();

    for (Map.Entry<String, SdkBindingData> entry : bindingData.entrySet()) {
      transform = transform.withInput(entry.getKey(), entry.getValue());
    }

    return transform;
  }

  @Override
  public SdkNode apply(
      SdkWorkflowBuilder builder,
      String nodeId,
      List<String> upstreamNodeIds,
      Map<String, SdkBindingData> inputs) {
    // TODO Auto-generated method stub
    return null;
  }
}
