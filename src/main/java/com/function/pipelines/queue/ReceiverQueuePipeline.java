package com.function.pipelines.queue;


import java.util.Base64;

import org.json.JSONObject;

import com.azure.storage.queue.QueueClient;
import com.azure.storage.queue.QueueClientBuilder;
import com.azure.storage.queue.models.QueueStorageException;
import com.microsoft.azure.functions.ExecutionContext;
import com.microsoft.azure.functions.OutputBinding;
import com.microsoft.azure.functions.annotation.BindingName;
import com.microsoft.azure.functions.annotation.BlobTrigger;
import com.microsoft.azure.functions.annotation.FunctionName;
import com.microsoft.azure.functions.annotation.QueueOutput;


import com.function.config.QueuePipelineConfig;
import com.function.config.PipelineConfig;
import com.function.pipelines.helper.Partitioner;
import com.function.config.AccountConfig;


/**
 * Listens on blob storage entries. Forwards task to Counter function, then turns to potential merger unit.
 * ////////////////////////////////
 * Invoked for a freshly uploaded blob. Following tasks are executed after invokation:
 * 1. Count number of lines present in blob.
 * 2. Split given task into subtasks.
 * 3. Send tasks into the tasks queue on which the counter function listens to.
 * 4. Issue aggregation task
 * 5. Terminate.
 */
public class ReceiverQueuePipeline {
    @FunctionName("ReciverQueuePipeline")
        public void run(
        @BlobTrigger(name = "file",
                    dataType = "binary",
                    path = QueuePipelineConfig.TASKS_BLOB_CONTAINER + "/{name}",
                    connection = "AzureWebJobsStorage") byte[] content,
        @BindingName("name") String filename,
        @QueueOutput(name= "out",
                    queueName = QueuePipelineConfig.AGGREGATION_QUEUE_NAME,
                    connection = "AzureWebJobsStorage") OutputBinding<String> message,
        final ExecutionContext context
        ) {
            // queue for issuing tasks            
            QueueClient tasksQueue = new QueueClientBuilder()
                                    .connectionString(AccountConfig.CONNECTION_STRING)
                                    .queueName(QueuePipelineConfig.TASKS_QUEUE_NAME)
                                    .buildClient();

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

            for (int i = 0; i < PipelineConfig.N_PARTITIONS; i++) {
                JSONObject jsonObject = Partitioner.getIthPartition(i, content, filename);
                jsonObject.put(PipelineConfig.JOB_CONTAINER_PROP, QueuePipelineConfig.TASKS_BLOB_CONTAINER);
                jsonObject.put(QueuePipelineConfig.RESULTS_QUEUE_NAME, resultClientName);
                jsonObject.put(PipelineConfig.AGGREGATION_ID, i);
                tasksQueue.sendMessage(Base64.getEncoder().encodeToString(jsonObject.toString().getBytes()));
            }

            // issue aggregation task
            JSONObject aggregationTask = new JSONObject()
                                .put(QueuePipelineConfig.FIRST_PARTITION, 0)
                                .put(QueuePipelineConfig.LAST_PARTITION, 9)
                                .put(QueuePipelineConfig.RESULTS_QUEUE_NAME, resultClientName);
            message.setValue(aggregationTask.toString());
        }
}
