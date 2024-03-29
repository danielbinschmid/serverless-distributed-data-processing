package com.function.pipelines.queue;

import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;

import com.azure.storage.blob.models.BlobRequestConditions;
import com.azure.storage.blob.models.BlockBlobItem;
import com.azure.storage.blob.options.BlobParallelUploadOptions;
import com.azure.storage.blob.specialized.BlobLeaseClient;
import com.azure.storage.blob.specialized.BlobLeaseClientBuilder;

import java.util.HashMap;
import java.util.Map;

import com.function.config.QueuePipelineConfig;
import com.function.pipelines.helper.BlobContainerWrapper;

import com.function.pipelines.helper.Merger;
import org.json.JSONObject;

import com.azure.core.http.rest.Response;
import com.azure.core.util.BinaryData;
import com.azure.core.util.Context;

import com.microsoft.azure.functions.ExecutionContext;
import com.microsoft.azure.functions.annotation.FunctionName;
import com.microsoft.azure.functions.annotation.QueueTrigger;

import java.math.BigDecimal;
import java.math.MathContext;

import com.function.config.PipelineConfig;
import com.function.config.AccountConfig;

/**
 * Merges result of a file into the total set of results.
 * 
 * 1. lease result blob
 * 2. Read current state from result blob
 * 3. update state with new result values
 * 4. upload new result content to result blob and result blob copy
 * 5. release result blob
 */
public class QueueMerger {
    private Map<String, Map<String, BigDecimal>> nationToSumCount;
    
    @FunctionName("QueueMerger")
    public void run(
            @QueueTrigger(name = "msg",
            queueName = QueuePipelineConfig.RESULTS_QUEUE_NAME,
            connection = "AzureWebJobsStorage") String message,
            final ExecutionContext context) {
        nationToSumCount = new HashMap<>();

        // 1. Lease result blob
        BlobServiceClient blobServiceClient = new BlobServiceClientBuilder()
                            .endpoint(AccountConfig.BLOB_STORAGE_ACC_ENDPOINT)
                            .sasToken(AccountConfig.STORAGE_ACC_SAS_TOKEN)
                            .buildClient();
        BlobContainerClient client = blobServiceClient.getBlobContainerClient(QueuePipelineConfig.RESULTS_BLOB_CONTAINER);
        BlobClient blobClient = client.getBlobClient(QueuePipelineConfig.FINAL_RESULTS_STATE_BLOB_NAME);

        // create results blob if it does not exist.
        if (!blobClient.exists()) {
            JSONObject state = new JSONObject()
                            .put(QueuePipelineConfig.NEW_COUNT_RESULT, new JSONObject())
                            .put(QueuePipelineConfig.RESULT_COUNTER, 0);
            BlobContainerWrapper resultBlobContainerWrapper = new BlobContainerWrapper(QueuePipelineConfig.RESULTS_BLOB_CONTAINER);
            resultBlobContainerWrapper.writeFile(QueuePipelineConfig.FINAL_RESULTS_STATE_BLOB_NAME, state.toString());
        }

        // lease existing or freshly created result blob
        BlobLeaseClient leaseClient = new BlobLeaseClientBuilder()
                        .blobClient(blobClient)
                        .buildClient();
        String leaseID = Merger.acquireLease(leaseClient, context);
        
        // 2. load state
        BlobContainerWrapper resultBlobContainerWrapper = new BlobContainerWrapper(QueuePipelineConfig.RESULTS_BLOB_CONTAINER);
        BinaryData currentStateBinary = resultBlobContainerWrapper.readFile(QueuePipelineConfig.FINAL_RESULTS_STATE_BLOB_NAME);
        JSONObject currentStateFull = new JSONObject(currentStateBinary.toString());
        JSONObject currentStateJSON = currentStateFull.getJSONObject(QueuePipelineConfig.NEW_COUNT_RESULT);
        long count = currentStateFull.getLong(QueuePipelineConfig.RESULT_COUNTER);

        for(String countryKey: currentStateJSON.keySet()) {    
            JSONObject countryData = currentStateJSON.getJSONObject(countryKey);
            BigDecimal countrySum = countryData.getBigDecimal(PipelineConfig.MERGE_RESULT_SUM);
            BigDecimal countryCount = countryData.getBigDecimal(PipelineConfig.MERGE_RESULT_COUNT);
            
            Map<String, BigDecimal> nestedMapwithSumAndCount = new HashMap<>();
            nestedMapwithSumAndCount.put(PipelineConfig.MERGE_RESULT_SUM, countrySum);
            nestedMapwithSumAndCount.put(PipelineConfig.MERGE_RESULT_COUNT, countryCount);
            nationToSumCount.put(countryKey, nestedMapwithSumAndCount);
        }

        // 3. update state
        JSONObject msgData = new JSONObject(message);
        JSONObject newCounts = msgData.getJSONObject(QueuePipelineConfig.NEW_COUNT_RESULT);

        for(String nationKey: newCounts.keySet()) {
            JSONObject nationData = newCounts.getJSONObject(nationKey);
            BigDecimal newNationSum = nationData.getBigDecimal(PipelineConfig.MERGE_RESULT_SUM);
            BigDecimal newNationCount = nationData.getBigDecimal(PipelineConfig.MERGE_RESULT_COUNT);
            
            if (nationToSumCount.containsKey(nationKey)) {
                BigDecimal currentSum = nationToSumCount.get(nationKey).get(PipelineConfig.MERGE_RESULT_SUM);
                BigDecimal currentCount = nationToSumCount.get(nationKey).get(PipelineConfig.MERGE_RESULT_COUNT);
                newNationSum = newNationSum.add(currentSum);
                newNationCount = newNationCount.add(currentCount);
            } 
            Map<String, BigDecimal> nestedMapwithSumAndCount = new HashMap<>();
            nestedMapwithSumAndCount.put(PipelineConfig.MERGE_RESULT_SUM, newNationSum);
            nestedMapwithSumAndCount.put(PipelineConfig.MERGE_RESULT_COUNT, newNationCount);
            nationToSumCount.put(nationKey, nestedMapwithSumAndCount);
        }

        // 4. upload
        JSONObject newState = new JSONObject()
                            .put(QueuePipelineConfig.NEW_COUNT_RESULT, nationToSumCount)
                            .put(QueuePipelineConfig.RESULT_COUNTER, count + 1l);
        
        try {   
            // update state of result blob
            BlobRequestConditions cond = new BlobRequestConditions().setLeaseId(leaseID);
            BlobParallelUploadOptions opts = new BlobParallelUploadOptions(BinaryData.fromString(newState.toString()));
            opts.setRequestConditions(cond);
            // Response<BlockBlobItem> response = 
            Response<BlockBlobItem> response = blobClient.uploadWithResponse(opts, null, Context.NONE);
            context.getLogger().info("MERGER upload status: " + String.valueOf(response.getStatusCode()));

            // compute averages and upload to output result blob
            BlobContainerWrapper resultCopy = new BlobContainerWrapper(QueuePipelineConfig.RESULTS_BLOB_CONTAINER);
            Map<String, BigDecimal> nationBalanceAverage = new HashMap<>();

            nationToSumCount.forEach( (nationKey, sumAndCountMap) -> {
                    BigDecimal nationSum = sumAndCountMap.get(PipelineConfig.MERGE_RESULT_SUM);
                    BigDecimal nationCount = sumAndCountMap.get(PipelineConfig.MERGE_RESULT_COUNT);
                    BigDecimal nationAverage = nationSum.divide(nationCount, MathContext.DECIMAL128);
                    nationBalanceAverage.put(nationKey, nationAverage);
                }
            );

            // JSONObject nationBalanceAverageResult = new JSONObject(nationBalanceAverage);
            JSONObject sharedResult = new JSONObject()
                    .put(QueuePipelineConfig.NEW_COUNT_RESULT, nationBalanceAverage)
                    .put(QueuePipelineConfig.RESULT_COUNTER, count);
            resultCopy.writeFile(QueuePipelineConfig.FINAL_RESULTS_OUTPUT_BLOB_NAME, sharedResult.toString());

            leaseClient.releaseLease();

        } catch (Exception e) {
            context.getLogger().info("EXCEPTION: " + e.getMessage());
        }                
    }

}