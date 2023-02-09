package com.function;

import java.time.OffsetDateTime;

import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.function.config.AccountConfig;
import com.function.config.BlobPipelineConfig;
import com.function.config.QueuePipelineConfig;

public class GetBenchmarkingResult {
    
    /**
     * 
     * @param args Usage: <trigger_blob_name> <queue_type>
     */
    public static void main(String[] args) {
        if (args.length != 2) {
            System.out.println("Usage: <trigger_blob_name> <queue_type>");
            return;
        }
        String triggerBlobName = args[0];
        String queueType = args[1];

        
        BlobServiceClient blobServiceClient = new BlobServiceClientBuilder()
                            .endpoint(AccountConfig.BLOB_STORAGE_ACC_ENDPOINT)
                            .sasToken(AccountConfig.STORAGE_ACC_SAS_TOKEN)
                            .buildClient();

        BlobContainerClient client;
        BlobClient resultsBlobClient;

        if (queueType.compareTo("queue") == 0) {
            client = blobServiceClient.getBlobContainerClient(QueuePipelineConfig.RESULTS_BLOB_CONTAINER);
            resultsBlobClient = client.getBlobClient(QueuePipelineConfig.FINAL_RESULTS_OUTPUT_BLOB_NAME);
        } else if(queueType.compareTo("blob") == 0){
            client = blobServiceClient.getBlobContainerClient(BlobPipelineConfig.MERGE_RESULT_CONTAINER_NAME);
            resultsBlobClient = client.getBlobClient(BlobPipelineConfig.MERGE_RESULT_FILENAME);
        } else {
            return;
        }

        BlobClient triggerBlobClient = client.getBlobClient(triggerBlobName);
            
        OffsetDateTime tEnd = resultsBlobClient.getProperties().getLastModified();
        OffsetDateTime tStart = triggerBlobClient.getProperties().getLastModified();

        System.out.println(tStart.toString());
        System.out.println(tEnd.toString());

        System.out.println((tEnd.getSecond() - tStart.getSecond()) + 60 * (tEnd.getMinute() - tStart.getMinute()) + 360 * (tEnd.getHour() - tStart.getHour()));
    }



}
