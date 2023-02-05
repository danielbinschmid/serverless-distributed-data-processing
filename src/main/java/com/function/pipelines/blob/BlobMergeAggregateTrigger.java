package com.function.pipelines.blob;

import com.azure.core.util.BinaryData;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.function.config.BlobPipelineConfig;
import com.function.config.PipelineConfig;
import com.function.pipelines.helper.Merger;
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
                    connection = "AzureWebJobsStorage") byte[] content
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
        BinaryData mergeResultBinary = writeContainer.readFile(BlobPipelineConfig.MERGE_RESULT_FILENAME);

        if (mergeResultBinary == null) {
            // If that's the first result, just dump the data
            JSONObject result = new JSONObject(nationKeyToSumAndCount);
            writeContainer.writeFile(BlobPipelineConfig.MERGE_RESULT_FILENAME, result.toString());
        } else {
            // We need to merge otherwise ^_^
            try {
                Merger.mergeResults(nationKeyToSumAndCount, mergeResultBinary);

                // We are done, so simply overwrite the file
                JSONObject result = new JSONObject(nationKeyToSumAndCount);
                writeContainer.writeFile(BlobPipelineConfig.MERGE_RESULT_FILENAME, result.toString());
            } catch (Exception e) {
                System.err.println("Something went wrong during the reading of the merge-result and the task execution");
            }
        }

    }

}

