package com.function;

import java.util.*;

import com.azure.core.util.BinaryData;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.queue.QueueClient;
import com.azure.storage.queue.QueueClientBuilder;
import com.azure.storage.queue.models.QueueStorageException;
import com.function.config.AccountConfig;
import com.function.config.PipelineConfig;
import com.function.config.QueuePipelineConfig;
import com.function.pipelines.helper.BlobContainerWrapper;
import com.function.pipelines.helper.Partitioner;
import com.microsoft.azure.functions.annotation.*;
import com.microsoft.azure.functions.*;
import org.json.JSONObject;

public class HttpEndpoint {
    
    private ExecutionContext context;
    private BlobServiceClient blobServiceClient;

    @FunctionName("HttpEndpoint")
    public HttpResponseMessage run(
            @HttpTrigger(name = "req", 
                    methods = {HttpMethod.GET}, 
                    authLevel = AuthorizationLevel.ANONYMOUS,
                    route = "event-driven-pipeline/{type:alpha}/{pipeline:alpha}") HttpRequestMessage<Optional<String>> request,
            @BindingName("type") String type,
            @BindingName("pipeline") String pipeline,
            final ExecutionContext context) {
        
        this.context = context;
        this.blobServiceClient = new BlobServiceClientBuilder()
                            .endpoint(AccountConfig.BLOB_STORAGE_ACC_ENDPOINT)
                            .sasToken(AccountConfig.STORAGE_ACC_SAS_TOKEN)
                            .buildClient();

        switch(type) {
            case "trigger":
                switch(pipeline) {
                    case "queue":
                        return request.createResponseBuilder(HttpStatus.OK).body(queuePipelineTrigger()).build();
                    case "blob":
                        break;
                    default:
                        return request.createResponseBuilder(HttpStatus.OK).body(getMenuMessage()).build();
                }
                break;
            case "result":
                switch(pipeline) {
                    case "queue":
                        return request.createResponseBuilder(HttpStatus.OK).body(getCurrentQueuePipelineResult()).build();
                    case "blob":
                        break;
                    default:
                        return request.createResponseBuilder(HttpStatus.OK).body(getMenuMessage()).build();
                }
                break;
            default:
                return request.createResponseBuilder(HttpStatus.OK).body(getMenuMessage()).build();
        }

        return request.createResponseBuilder(HttpStatus.OK).body("not implemented yet").build();
    }


    private String getMenuMessage() {
        return "event-driven-pipeline/trigger/<pipeline_type> for triggering a pipeline\n"
            + "event-driven-pipeline/result/<pipeline_type> for fetching the current result of a pipeline\n"
            + "pipeline_type -> 'queue' for queue based pipeline\n"
            + "pipeline_type -> 'blob' for blob based pipeline";
    }

    private String getCurrentQueuePipelineResult() {
        // download result_avg.json in container results
        BlobContainerClient containerClient = blobServiceClient.getBlobContainerClient(QueuePipelineConfig.RESULTS_BLOB_CONTAINER);
        BlobClient blobClient = containerClient.getBlobClient(QueuePipelineConfig.FINAL_RESULTS_OUTPUT_BLOB_NAME);
        if (blobClient.exists()) {
            BinaryData content = blobClient.downloadContent();
            // JSONObject currentStateJSON = new JSONObject(content.toString()).getJSONObject(QueuePipelineConfig.NEW_COUNT_RESULT);
            return content.toString();
        } else {
            return "no result has yet been computed";
        }
    }


    private StringBuilder queuePipelineTrigger() {
        // queue for issuing tasks            
        QueueClient tasksQueue = new QueueClientBuilder()
                .connectionString(AccountConfig.CONNECTION_STRING)
                .queueName(QueuePipelineConfig.TASKS_QUEUE_NAME)
                .buildClient();

        QueueClient aggregationClient = new QueueClientBuilder()
                .connectionString(AccountConfig.CONNECTION_STRING)
                .queueName(QueuePipelineConfig.AGGREGATION_QUEUE_NAME)
                .buildClient();
            
        BlobContainerWrapper uploadContainer = new BlobContainerWrapper(PipelineConfig.FILE_LIST_CONTAINER_NAME);

        List<String> listOfSkippedFiles = new ArrayList<>();

        uploadContainer.getAllBlobs().forEach(blob -> {
            // create queue to enqueue aggregationresults into
            String resultClientName = "result" + java.util.UUID.randomUUID();
            QueueClient aggregationResultClient = new QueueClientBuilder()
                .connectionString(AccountConfig.CONNECTION_STRING)
                .queueName(resultClientName)
                .buildClient();

            try { 
                aggregationResultClient.create(); 
            } catch (QueueStorageException e) {
                context.getLogger().info("Error code: " + e.getErrorCode() + "Message: " + e.getMessage()); 
            }


            BinaryData binaryData = uploadContainer.readFile(blob.getName());
            if (binaryData == null) {
                // TODO: Is skipping the file a good approach?
                System.err.println("Something went wrong while trying to read: " + blob.getName());
                listOfSkippedFiles.add(blob.getName());
                return;
            }

            for (int i = 0; i < PipelineConfig.N_PARTITIONS; i++) {
                JSONObject jsonObject = Partitioner.getIthPartition(i, binaryData.toBytes(), blob.getName());
                jsonObject.put(PipelineConfig.JOB_CONTAINER_PROP, PipelineConfig.FILE_LIST_CONTAINER_NAME);
                jsonObject.put(QueuePipelineConfig.RESULTS_QUEUE_NAME, resultClientName);
                jsonObject.put(PipelineConfig.AGGREGATION_ID, i);
                tasksQueue.sendMessage(Base64.getEncoder().encodeToString(jsonObject.toString().getBytes()));
            }

            // issue aggregation task
            JSONObject aggregationTask = new JSONObject()
                    .put(QueuePipelineConfig.FIRST_PARTITION, 0)
                    .put(QueuePipelineConfig.LAST_PARTITION, PipelineConfig.N_PARTITIONS - 1)
                    .put(QueuePipelineConfig.RESULTS_QUEUE_NAME, resultClientName);
            aggregationClient.sendMessage(Base64.getEncoder().encodeToString(aggregationTask.toString().getBytes()));
        });

        StringBuilder response = new StringBuilder();
        response.append("Pipeline was successfully triggered.\n")
                .append("List of skipped files: ")
                .append(listOfSkippedFiles);

        return response;
    }

}
