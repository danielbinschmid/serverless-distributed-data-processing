package com.function;

import com.function.config.AccountConfig;
import com.function.helper.Partitioner;
import com.microsoft.azure.functions.ExecutionContext;
import com.microsoft.azure.functions.annotation.*;
import org.json.JSONObject;

public class BlobUploadTrigger {

    @FunctionName("BlobUploadTrigger")
    public void run(
            @BlobTrigger(name = "file",
                    dataType = "binary",
                    path = AccountConfig.FILE_LIST_CONTAINER_NAME + "/{name}",
                    connection = "AzureWebJobsStorage") byte[] content,
            @BindingName("name") String filename,
            final ExecutionContext context
    ) {
        if (AccountConfig.DISABLE_BLOB_UPLOAD_TRIGGER) {
            return;
        }

        BlobContainerWrapper aggregationJobsContainer = new BlobContainerWrapper(AccountConfig.AGGREGATION_JOBS_CONTAINER_NAME);

        // Create all of the partitions and write them
        for (int i = 0; i < AccountConfig.N_PARTITIONS; i++) {
            JSONObject jsonObject = Partitioner.getIthPartition(i, content, filename);
            jsonObject.put(AccountConfig.JOB_CONTAINER_PROP, AccountConfig.FILE_LIST_CONTAINER_NAME);
            aggregationJobsContainer.writeFile(filename + "." + i + ".json", jsonObject.toString());
        }

        context.getLogger().info("Processed file: " + filename);

    }

}
