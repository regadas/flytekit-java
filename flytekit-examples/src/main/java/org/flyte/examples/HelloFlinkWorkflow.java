package org.flyte.examples;

import org.flyte.flytekit.SdkWorkflow;
import org.flyte.flytekit.SdkWorkflowBuilder;

public class HelloFlinkWorkflow extends SdkWorkflow {

    @Override
    public void expand(SdkWorkflowBuilder builder) {
        // builder.apply("flinkExample", RunFlinkJob)

    }
    
}
