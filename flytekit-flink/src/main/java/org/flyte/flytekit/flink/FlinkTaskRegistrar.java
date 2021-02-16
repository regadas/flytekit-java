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
import com.google.protobuf.util.JsonFormat;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.flyte.api.v1.Literal;
import org.flyte.api.v1.RetryStrategy;
import org.flyte.api.v1.RunnableTask;
import org.flyte.api.v1.RunnableTaskRegistrar;
import org.flyte.api.v1.TaskIdentifier;
import org.flyte.api.v1.TypedInterface;
import org.flyte.flytekit.SdkConfig;

/** A registrar that creates {@link FlinkTask} instances. */
@AutoService(RunnableTaskRegistrar.class)
public class FlinkTaskRegistrar extends RunnableTaskRegistrar {
  private static final Logger LOG = Logger.getLogger(FlinkTaskRegistrar.class.getName());

  static {
    // enable all levels for the actual handler to pick up
    LOG.setLevel(Level.ALL);
  }

  private static class RunnableTaskImpl implements RunnableTask {
    private static final String TASK_TYPE = "flink";
    private static final JsonFormat.Printer JSON_PRINTER = JsonFormat.printer();

    private final FlinkTask flinkTask;

    private RunnableTaskImpl(FlinkTask flinkTask) {
      this.flinkTask = flinkTask;
    }

    @Override
    public TypedInterface getInterface() {
      return TypedInterface.builder()
          .inputs(Collections.emptyMap())
          .outputs(Collections.emptyMap())
          .build();
    }

    @Override
    public Map<String, Literal> run(Map<String, Literal> inputs) {
      return Collections.emptyMap();
    }

    @Override
    public RetryStrategy getRetries() {
      return RetryStrategy.builder().retries(flinkTask.getRetries()).build();
    }

    @Override
    public String getName() {
      return flinkTask.getName();
    }

    @Override
    public String getType() {
      return TASK_TYPE;
    }

    @Override
    public String getCustom() {
      try {
        return JSON_PRINTER.print(flinkTask.getFlinkJob());
      } catch (Exception e) {
        throw new IllegalArgumentException(e);
      }
    }
  }

  @Override
  public Map<TaskIdentifier, RunnableTask> load(Map<String, String> env, ClassLoader classLoader) {
    ServiceLoader<FlinkTask> loader = ServiceLoader.load(FlinkTask.class, classLoader);

    LOG.fine("Discovering SdkRunnableTask");

    Map<TaskIdentifier, RunnableTask> tasks = new HashMap<>();
    SdkConfig sdkConfig = SdkConfig.load(env);

    for (FlinkTask sdkTask : loader) {
      String name = sdkTask.getName();
      TaskIdentifier taskId =
          TaskIdentifier.builder()
              .domain(sdkConfig.domain())
              .project(sdkConfig.project())
              .name(name)
              .version(sdkConfig.version())
              .build();
      LOG.fine(String.format("Discovered [%s]", name));

      RunnableTask task = new RunnableTaskImpl(sdkTask);
      RunnableTask previous = tasks.put(taskId, task);

      if (previous != null) {
        throw new IllegalArgumentException(
            String.format("Discovered a duplicate task [%s] [%s] [%s]", name, task, previous));
      }
    }

    return tasks;
  }
}
