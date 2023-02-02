package com.function;

import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;

import com.azure.storage.blob.models.BlobRequestConditions;
import com.azure.storage.blob.options.BlobParallelUploadOptions;
import com.azure.storage.blob.specialized.BlobLeaseClient;
import com.azure.storage.blob.specialized.BlobLeaseClientBuilder;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;


import org.json.JSONObject;

import com.azure.core.util.BinaryData;
import com.azure.core.util.Context;

import com.microsoft.azure.functions.ExecutionContext;
import com.microsoft.azure.functions.annotation.FunctionName;
import com.microsoft.azure.functions.annotation.QueueTrigger;

import java.math.BigDecimal;
import java.time.Duration;

import com.function.config.Config;
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
            queueName = Config.RESULTS_QUEUE_NAME,
            connection = "AzureWebJobsStorage") String message,
            final ExecutionContext context) {
        nationToSumCount = new HashMap<>();

        // 1. Lease result blob
        BlobServiceClient blobServiceClient = new BlobServiceClientBuilder()
                            .endpoint(AccountConfig.BLOB_STORAGE_ACC_ENDPOINT)
                            .sasToken(AccountConfig.BLOB_STORAGE_ACC_SAS_TOKEN)
                            .buildClient();
        BlobContainerClient client = blobServiceClient.getBlobContainerClient(Config.RESULTS_BLOB_CONTAINER);
        BlobClient blobClient = client.getBlobClient(Config.FINAL_RESULTS_STATE_BLOB_NAME);

        // create results blob if it does not exist.
        if (!blobClient.exists()) {
            JSONObject state = new JSONObject()
                            .put(Config.NEW_COUNT_RESULT, new JSONObject());
            BlobContainerWrapper resultBlobContainerWrapper = new BlobContainerWrapper(Config.RESULTS_BLOB_CONTAINER);
            resultBlobContainerWrapper.writeFile(Config.FINAL_RESULTS_STATE_BLOB_NAME, state.toString());
        }

        // lease existing or freshly created result blob
        BlobLeaseClient leaseClient = new BlobLeaseClientBuilder()
                        .blobClient(blobClient)
                        .buildClient();
        String leaseID = acquireLease(leaseClient, context);
        
        // 2. load state
        BlobContainerWrapper resultBlobContainerWrapper = new BlobContainerWrapper(Config.RESULTS_BLOB_CONTAINER);
        BinaryData currentStateBinary = resultBlobContainerWrapper.readFile(Config.FINAL_RESULTS_STATE_BLOB_NAME);
        JSONObject currentStateJSON = new JSONObject(currentStateBinary.toString()).getJSONObject(Config.NEW_COUNT_RESULT);
        
        for(String countryKey: currentStateJSON.keySet()) {    
            JSONObject countryData = currentStateJSON.getJSONObject(countryKey);
            BigDecimal countrySum = countryData.getBigDecimal(Config.MERGE_RESULT_SUM);
            BigDecimal countryCount = countryData.getBigDecimal(Config.MERGE_RESULT_COUNT);
            
            Map<String, BigDecimal> nestedMapwithSumAndCount = new HashMap<>();
            nestedMapwithSumAndCount.put(Config.MERGE_RESULT_SUM, countrySum);
            nestedMapwithSumAndCount.put(Config.MERGE_RESULT_COUNT, countryCount);
            nationToSumCount.put(countryKey, nestedMapwithSumAndCount);
        }

        // 3. update state
        JSONObject msgData = new JSONObject(message);
        JSONObject newCounts = msgData.getJSONObject(Config.NEW_COUNT_RESULT);

        for(String nationKey: newCounts.keySet()) {
            JSONObject nationData = newCounts.getJSONObject(nationKey);
            BigDecimal newNationSum = nationData.getBigDecimal(Config.MERGE_RESULT_SUM);
            BigDecimal newNationCount = nationData.getBigDecimal(Config.MERGE_RESULT_COUNT);
            
            if (nationToSumCount.containsKey(nationKey)) {
                BigDecimal currentSum = nationToSumCount.get(nationKey).get(Config.MERGE_RESULT_SUM);
                BigDecimal currentCount = nationToSumCount.get(nationKey).get(Config.MERGE_RESULT_COUNT);
                newNationSum.add(currentSum);
                newNationCount.add(currentCount);
            } 
            Map<String, BigDecimal> nestedMapwithSumAndCount = new HashMap<>();
            nestedMapwithSumAndCount.put(Config.MERGE_RESULT_SUM, newNationSum);
            nestedMapwithSumAndCount.put(Config.MERGE_RESULT_COUNT, newNationCount);
            nationToSumCount.put(nationKey, nestedMapwithSumAndCount);
        }

        // 4. upload
        JSONObject newState = new JSONObject()
                            .put(Config.NEW_COUNT_RESULT, nationToSumCount);
        
        try {   
            // update state of result blob
            BlobRequestConditions cond = new BlobRequestConditions().setLeaseId(leaseID);
            BlobParallelUploadOptions opts = new BlobParallelUploadOptions(BinaryData.fromString(newState.toString()));
            opts.setRequestConditions(cond);
            // Response<BlockBlobItem> response = 
            blobClient.uploadWithResponse(opts, Duration.ofMillis(500), Context.NONE);
            
            // compute averages and upload to output result blob
            BlobContainerWrapper resultCopy = new BlobContainerWrapper(Config.RESULTS_BLOB_CONTAINER);
            Map<String, BigDecimal> nationBalanceAverage = new HashMap<>();

            nationToSumCount.forEach( (nationKey, sumAndCountMap) -> {
                    BigDecimal nationSum = sumAndCountMap.get(Config.MERGE_RESULT_SUM);
                    BigDecimal nationCount = sumAndCountMap.get(Config.MERGE_RESULT_COUNT);
                    BigDecimal nationAverage = nationSum.divide(nationCount);
                    nationBalanceAverage.put(nationKey, nationAverage);
                }
            );

            JSONObject nationBalanceAverageResult = new JSONObject(nationBalanceAverage);
            resultCopy.writeFile(Config.FINAL_RESULTS_OUTPUT_BLOB_NAME, nationBalanceAverageResult.toString());

            leaseClient.releaseLease();

        } catch (Exception e) {
            context.getLogger().info("EXCEPTION: " + e.getMessage());
        }                
    }

    /**
     * Recursive lease attempt.
     * 
     * @param leaseClient
     * @param context
     * @return
     */
    private String acquireLease(BlobLeaseClient leaseClient, ExecutionContext context) {
        try 
        {
            return leaseClient.acquireLease(20);
        } 
        catch (Exception e) 
        {
            context.getLogger().info("blob can not be leased. Trying again. " + e.getMessage());
            try { 
                Thread.sleep(500);
                return acquireLease(leaseClient, context);
            } catch (Exception e2) {
                System.err.println(e2.getMessage());
            }    
        }  
        return "";
    }
}