package com.function;

import com.function.config.Config;
import com.function.helper.Partitioner;
import com.microsoft.azure.functions.ExecutionContext;
import com.microsoft.azure.functions.annotation.*;
import org.json.JSONObject;

public class BlobUploadTrigger {

    @FunctionName("BlobUploadTrigger")
    public void run(
            @BlobTrigger(name = "file",
                    dataType = "binary",
                    path = Config.FILE_LIST_CONTAINER_NAME + "/{name}",
                    connection = "AzureWebJobsStorage") byte[] content,
            @BindingName("name") String filename,
            final ExecutionContext context
    ) {
        BlobContainerWrapper aggregationJobsContainer = new BlobContainerWrapper(Config.AGGREGATION_JOBS_CONTAINER_NAME);

        // Create all of the partitions and write them
        for (int i = 0; i < Config.N_PARTITIONS; i++) {
            JSONObject jsonObject = Partitioner.getIthPartition(i, content, filename);
            jsonObject.put(Config.JOB_CONTAINER_PROP, Config.FILE_LIST_CONTAINER_NAME);
            aggregationJobsContainer.writeFile(filename + "." + i + ".json", jsonObject.toString());
        }

        context.getLogger().info("Processed file: " + filename);

    }

}
