package com.function.pipelines.blob;

import com.azure.core.http.rest.Response;
import com.azure.core.util.BinaryData;
import com.azure.core.util.Context;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.models.BlobRequestConditions;
import com.azure.storage.blob.models.BlockBlobItem;
import com.azure.storage.blob.options.BlobParallelUploadOptions;
import com.azure.storage.blob.specialized.BlobLeaseClient;
import com.azure.storage.blob.specialized.BlobLeaseClientBuilder;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.function.config.BlobPipelineConfig;
import com.function.config.PipelineConfig;
import com.function.pipelines.helper.BlobContainerWrapper;
import com.function.pipelines.helper.Merger;
import com.microsoft.azure.functions.ExecutionContext;
import com.microsoft.azure.functions.annotation.BlobTrigger;
import com.microsoft.azure.functions.annotation.FunctionName;
import org.json.JSONObject;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;

public class BlobMergeAggregateTrigger {
    @FunctionName("BlobMergeAggregateTrigger")
    public void run(
            @BlobTrigger(name = "file",
                    dataType = "binary",
                    // customer.00.1.result.json
                    path = BlobPipelineConfig.MERGING_JOBS_CONTAINER_NAME + "/{prefixname}.{ext}",
                    connection = "AzureWebJobsStorage") byte[] content,
            final ExecutionContext context
    ) {
        StringBuilder fileContent = new StringBuilder();
        for (byte b : content) {
            fileContent.append((char) b);
        }
        JSONObject jsonObject = new JSONObject(fileContent.toString());

        String filename = jsonObject.getString(PipelineConfig.MERGING_JOB_PREFIX);
        String container = jsonObject.getString(PipelineConfig.JOB_CONTAINER_PROP);

        if (filename == null || container == null) {
            System.err.println("Something went wrong. The job file couldn't be parsed.");
            return;
        }

        BlobContainerWrapper readContainer = new BlobContainerWrapper(container);

        Map<String, Map<String, BigDecimal>> nationKeyToSumAndCount = new HashMap<>();

        readContainer.getAllBlobs().forEach(blob -> {
            // Read all blobs that have this prefix
            if (blob.getName().contains(filename)) {
                BinaryData binaryData = readContainer.readFile(blob.getName());
                try {
                    // Try to aggregate the results
                    Merger.mergeResults(nationKeyToSumAndCount, binaryData);
                } catch (JsonProcessingException e) {
                    throw new RuntimeException(e);
                }
            }
        });


        // We should be ready and everything should be merged
        BlobContainerWrapper writeContainer = new BlobContainerWrapper(BlobPipelineConfig.MERGE_RESULT_CONTAINER_NAME);
        // Check if there is a result already. If that's the case merge the results.
        BlobClient blobClient = writeContainer.getBlobClient(BlobPipelineConfig.MERGE_RESULT_FILENAME);

        if (!blobClient.exists()) {
            JSONObject result = new JSONObject(nationKeyToSumAndCount);
            writeContainer.writeFile(BlobPipelineConfig.MERGE_RESULT_FILENAME, result.toString());
        } else {
            try {
                BlobLeaseClient blobLeaseClient = new BlobLeaseClientBuilder()
                        .blobClient(blobClient)
                        .buildClient();
                String leaseId = Merger.acquireLease(blobLeaseClient, context);

                BinaryData mergeResultBinary = writeContainer.readFile(BlobPipelineConfig.MERGE_RESULT_FILENAME);
                Merger.mergeResults(nationKeyToSumAndCount, mergeResultBinary);
                // We are done, so simply overwrite the file
                JSONObject result = new JSONObject(nationKeyToSumAndCount);

                BlobRequestConditions cond = new BlobRequestConditions().setLeaseId(leaseId);
                BlobParallelUploadOptions opts = new BlobParallelUploadOptions(BinaryData.fromString(result.toString()));
                opts.setRequestConditions(cond);
                Response<BlockBlobItem> response = blobClient.uploadWithResponse(opts, null, Context.NONE);
                context.getLogger().info("MERGER upload status: " + String.valueOf(response.getStatusCode()));

                blobLeaseClient.releaseLease();
            } catch (Exception e) {
                context.getLogger().info("Something went wrong during the reading of the merge-result and the task execution");
            }
        }
    }
}
