package com.function;


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
    
    @FunctionName("receiver")
        public void run(
        @BlobTrigger(name = "file",
                    dataType = "binary",
                    path = Config.TASKS_BLOB_CONTAINER + "/{name}",
                    connection = "AzureWebJobsStorage") byte[] content,
        @BindingName("name") String filename,
        @QueueOutput(name= "out",
                    queueName = Config.AGGREGATION_QUEUE_NAME,
                    connection = "AzureWebJobsStorage") OutputBinding<String> message,
        final ExecutionContext context
        ) {
            // queue for issuing tasks            
            QueueClient tasksQueue = new QueueClientBuilder()
                                    .connectionString(Config.CONNECTION_STRING)
                                    .queueName(Config.TASKS_QUEUE_NAME)
                                    .buildClient();

            // create queue to enqueue aggregationresults into
            String resultClientName = "result" + java.util.UUID.randomUUID();
            QueueClient aggregationResultClient = new QueueClientBuilder()
                                .connectionString(Config.CONNECTION_STRING)
                                .queueName(resultClientName)
                                .buildClient();
            try { aggregationResultClient.create(); }
            catch (QueueStorageException e) { context.getLogger().info("Error code: " + e.getErrorCode() + "Message: " + e.getMessage()); }

            // range length and number of lines of file
            double n = 0.0;
            for (byte b : content) { if (b == '\n') n++; }
            double rangeLength = n / 10.0;

            for (int i = 0; i < 10; i++) {
                // compute range end and range start.
                long rangeStart = (long) Math.floor(i * rangeLength);
                long rangeEnd = (long) Math.floor((i + 1) * rangeLength);
                if (rangeEnd > n) rangeEnd =(long) n;
                if (i == 9 && rangeEnd < n - 1) rangeEnd = (long) n - 1;
                
                // issue task to tasks-queue
                JSONObject jsonObject = new JSONObject()
                                .put("target", filename)
                                .put("rangeStart", rangeStart)
                                .put("rangeEnd", rangeEnd)
                                .put("resultqueueName", resultClientName)
                                .put("aggregationID", i)
                                .put("container", Config.TASKS_BLOB_CONTAINER); 
                tasksQueue.sendMessage(Base64.getEncoder().encodeToString(jsonObject.toString().getBytes()));
            }

            // issue aggregation task
            JSONObject aggregationTask = new JSONObject()
                                .put("firstPartition", 0l)
                                .put("lastPartition", 9l)
                                .put("resultqueue", resultClientName);
            message.setValue(aggregationTask.toString());
        }
}
