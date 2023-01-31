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


import org.javatuples.Pair;
import org.json.JSONObject;

import com.azure.core.util.BinaryData;
import com.azure.core.util.Context;

import com.microsoft.azure.functions.ExecutionContext;
import com.microsoft.azure.functions.annotation.FunctionName;
import com.microsoft.azure.functions.annotation.QueueTrigger;
import java.time.Duration;


/**
 * Merges result of a file into the total set of results.
 * 
 * 1. lease result blob
 * 2. Read current state from result blob
 * 3. update state with new result values
 * 4. upload new result content to result blob and result blob copy
 * 5. release result blob
 */
public class Merger {
    private Map<String, Pair<Double, Long>> nationToSumCount;
    
    @FunctionName("merger")
    public void run(
            @QueueTrigger(name = "msg",
            queueName = Config.RESULTS_QUEUE_NAME,
            connection = "AzureWebJobsStorage") String message,
            final ExecutionContext context) {
        nationToSumCount = new HashMap<>();

        // 1. Lease result blob
        BlobServiceClient blobServiceClient = new BlobServiceClientBuilder()
                            .endpoint(Config.BLOB_STORAGE_ACC_ENDPOINT)
                            .sasToken(Config.BLOB_STORAGE_ACC_SAS_TOKEN)
                            .buildClient();
        BlobContainerClient client = blobServiceClient.getBlobContainerClient(Config.RESULTS_BLOB_CONTAINER);
        BlobClient blobClient = client.getBlobClient(Config.FINAL_RESULTS_STATE_BLOB_NAME);
        // create results blob if it does not exist.
        if (!blobClient.exists()) {
            JSONObject state = new JSONObject()
                            .put("result", new JSONObject());
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
        JSONObject currentStateJSON = new JSONObject(currentStateBinary.toString()).getJSONObject("result");
        
        for(String countryKey: currentStateJSON.keySet()) {    
            JSONObject countryData = currentStateJSON.getJSONObject(countryKey);
            double sum = countryData.getDouble("value0");
            long count = countryData.getLong("value1");
            
            nationToSumCount.put(countryKey, new Pair<Double,Long>(sum, count));
        }

        // 3. update state
        JSONObject msgData = new JSONObject(message);
        JSONObject newCounts = msgData.getJSONObject("result");
        Set<String> countryKeys = newCounts.keySet();

        for(String countryKey: countryKeys) {
            JSONObject countryData = newCounts.getJSONObject(countryKey);
            double sum = countryData.getDouble("value0");
            long count = countryData.getLong("value1");

            
            if (nationToSumCount.containsKey(countryKey)) {
                sum += nationToSumCount.get(countryKey).getValue0();
                count += nationToSumCount.get(countryKey).getValue1();
            } 
            nationToSumCount.put(countryKey, new Pair<Double,Long>(sum, count));
        }

        // 4. upload
        JSONObject newState = new JSONObject()
                            .put("result", nationToSumCount);
        
        try {   
            // update state of result blob
            BlobRequestConditions cond = new BlobRequestConditions().setLeaseId(leaseID);
            BlobParallelUploadOptions opts = new BlobParallelUploadOptions(BinaryData.fromString(newState.toString()));
            opts.setRequestConditions(cond);
            // Response<BlockBlobItem> response = 
            blobClient.uploadWithResponse(opts, Duration.ofMillis(500), Context.NONE);
            
            // compute averages and upload to output result blob
            BlobContainerWrapper resultCopy = new BlobContainerWrapper(Config.RESULTS_BLOB_CONTAINER);
            HashMap<String, Double> avgs = new HashMap<>();
            for(Map.Entry<String, Pair<Double, Long>> entry: nationToSumCount.entrySet()) {
                avgs.put(entry.getKey(), (double) entry.getValue().getValue0() / (double) entry.getValue().getValue1());
            }
            JSONObject avgsRes = new JSONObject(avgs);
            resultCopy.writeFile(Config.FINAL_RESULTS_OUTPUT_BLOB_NAME, avgsRes.toString());

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