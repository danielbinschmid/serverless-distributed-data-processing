package com.function;

import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.queue.QueueClient;
import com.azure.storage.queue.QueueClientBuilder;
import com.azure.storage.queue.models.QueueStorageException;

import com.function.config.QueuePipelineConfig;
import com.function.config.BlobPipelineConfig;
import com.function.config.PipelineConfig;
import com.function.config.AccountConfig;

public class Initialization {

    public static void main(String[] args) {
        // service client for blobs
        BlobServiceClient blobServiceClient = new BlobServiceClientBuilder()
                            .endpoint(AccountConfig.BLOB_STORAGE_ACC_ENDPOINT)
                            .sasToken(AccountConfig.STORAGE_ACC_SAS_TOKEN)
                            .buildClient();

        // Container where the files are uploaded
        createContainer(blobServiceClient, PipelineConfig.FILE_LIST_CONTAINER_NAME);

        Initialization.initQueuePipeline(blobServiceClient);
        Initialization.initBlobPipeline(blobServiceClient);
    }


    private static void initQueuePipeline(BlobServiceClient blobServiceClient) {
        System.out.println("Setting up Azure Queues based pipeline..");

        // ------ set up blob container ------------

        // create results container
        createContainer(blobServiceClient, QueuePipelineConfig.RESULTS_BLOB_CONTAINER);

        // TODO: init empty results

        // ------------ create queues ---------------
        // queue for partitioned tasks
        createQueue(QueuePipelineConfig.TASKS_QUEUE_NAME);

        // queue for aggregation of partitioned tasks
        createQueue(QueuePipelineConfig.AGGREGATION_QUEUE_NAME);

        // queue for results
        createQueue(QueuePipelineConfig.RESULTS_QUEUE_NAME);
    }

    private static void initBlobPipeline(BlobServiceClient blobServiceClient) {
        System.out.println("Setting up Azure Blob only based pipeline..");

        

        // Container where the aggregation job descriptions are uploaded
        createContainer(blobServiceClient, BlobPipelineConfig.AGGREGATION_JOBS_CONTAINER_NAME);

        // Container where the aggregation results are uploaded
        createContainer(blobServiceClient, BlobPipelineConfig.AGGREGATION_RESULTS_CONTAINER_NAME);

        // Container where the merging jobs descriptions are uploaded
        createContainer(blobServiceClient, BlobPipelineConfig.MERGING_JOBS_CONTAINER_NAME);

        // Container where the merge result is uploaded
        createContainer(blobServiceClient, BlobPipelineConfig.MERGE_RESULT_CONTAINER_NAME);

        // TODO: init empty results
    }

    private static void createQueue(String queueName) {
        QueueClient queueClient = new QueueClientBuilder()
                                .connectionString(AccountConfig.CONNECTION_STRING)
                                .queueName(queueName)
                                .buildClient();
        try { 
            queueClient.create(); 
            System.out.println("Successfully created " + queueName + " queue.");
        } catch (QueueStorageException e) {
            System.out.println("Error while creating queue. See error message below.");
            System.err.println(e.getMessage());
        }
    }

    private static void createContainer(BlobServiceClient blobServiceClient, String containerName) {
        BlobContainerClient containerClient = blobServiceClient.getBlobContainerClient(containerName);
        if (!containerClient.exists()) {
            containerClient.create();
            System.out.println("Successfully created " + containerName + " container");
        } else {
            System.out.println(containerName + " container already exists.");
        }
    }

}
