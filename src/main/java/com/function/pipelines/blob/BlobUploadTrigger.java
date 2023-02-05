package com.function.pipelines.blob;


import com.function.config.BlobPipelineConfig;
import com.function.config.PipelineConfig;
import com.function.pipelines.helper.Partitioner;
import com.microsoft.azure.functions.ExecutionContext;
import com.microsoft.azure.functions.annotation.*;
import org.json.JSONObject;

public class BlobUploadTrigger {

    @FunctionName("BlobUploadTrigger")
    public void run(
            @BlobTrigger(name = "file",
                    dataType = "binary",
                    path = BlobPipelineConfig.FILE_LIST_CONTAINER_NAME + "/{name}",
                    connection = "AzureWebJobsStorage") byte[] content,
            @BindingName("name") String filename,
            final ExecutionContext context
    ) {
        if (PipelineConfig.DISABLE_BLOB_UPLOAD_TRIGGER) {
            return;
        }

        BlobContainerWrapper aggregationJobsContainer = new BlobContainerWrapper(BlobPipelineConfig.AGGREGATION_JOBS_CONTAINER_NAME);

        // Create all of the partitions and write them
        for (int i = 0; i < PipelineConfig.N_PARTITIONS; i++) {
            JSONObject jsonObject = Partitioner.getIthPartition(i, content, filename);
            jsonObject.put(PipelineConfig.JOB_CONTAINER_PROP, BlobPipelineConfig.FILE_LIST_CONTAINER_NAME);
            aggregationJobsContainer.writeFile(filename + "." + i + ".json", jsonObject.toString());
        }

        context.getLogger().info("Processed file: " + filename);

    }

}
