package com.function.pipelines.queue;

import java.math.BigDecimal;
import java.util.Base64;
import java.util.Map;

import org.json.JSONException;
import org.json.JSONObject;
import com.function.config.QueuePipelineConfig;
import com.azure.core.util.BinaryData;
import com.azure.storage.queue.QueueClient;
import com.azure.storage.queue.QueueClientBuilder;
import com.microsoft.azure.functions.ExecutionContext;
import com.microsoft.azure.functions.annotation.FunctionName;
import com.microsoft.azure.functions.annotation.QueueTrigger;
import com.function.config.PipelineConfig;
import com.function.pipelines.helper.BlobContainerWrapper;
import com.function.pipelines.helper.Counter;
import com.function.config.AccountConfig;


/**
 * Listens on the taskqueue. 
 * 
 * Completes task and streams result to mainqueue. Issues merge request after result streaming in mergerequests queue.
 */
public class QueueCounter {

    @FunctionName("QueueCounter")
    public void run(
            @QueueTrigger(name = "msg",
            queueName = QueuePipelineConfig.TASKS_QUEUE_NAME,
            connection = "AzureWebJobsStorage") String message,
            final ExecutionContext context) {
        JSONObject jsonObject = new JSONObject(message);

        try {
            String filename = jsonObject.getString(PipelineConfig.AGGREGATION_JOB_TARGET);
            context.getLogger().info("COUNTER: Received " + filename + " as task.");
            String resultqueuename = jsonObject.getString(QueuePipelineConfig.RESULTS_QUEUE_NAME);

            QueueClient resultClient = new QueueClientBuilder()
                                .connectionString(AccountConfig.CONNECTION_STRING)
                                .queueName(resultqueuename)
                                .buildClient();
            
            BlobContainerWrapper blobContainerWrapper = new BlobContainerWrapper(jsonObject.getString(PipelineConfig.JOB_CONTAINER_PROP));
            BinaryData file = blobContainerWrapper.readFile(filename);

            if (file != null) {
                Map<String, Map<String, BigDecimal>> nationToSumCount = Counter.findCountAndSum(file, -1, -1);

                // TODO: logic for assumption, that a single results fits into the 64kB restriction of Azure queues. 
                JSONObject res = new JSONObject()
                            .put(PipelineConfig.TYPE_OF_CONTENT, QueuePipelineConfig.NEW_COUNT_RESULT)
                            .put(PipelineConfig.AGGREGATION_ID, filename)
                            .put(QueuePipelineConfig.NEW_COUNT_RESULT, nationToSumCount);

                resultClient.sendMessage(Base64.getEncoder().encodeToString(res.toString().getBytes()));
            } else {
                context.getLogger().info("Something went wrong! We could not read the file " + filename);
                
                // re-issue to queue
                QueueClient tasksQueue = new QueueClientBuilder()
                        .connectionString(AccountConfig.CONNECTION_STRING)
                        .queueName(QueuePipelineConfig.TASKS_QUEUE_NAME)
                        .buildClient();
                tasksQueue.sendMessage(Base64.getEncoder().encodeToString(jsonObject.toString().getBytes()));

            }
        } 
        catch (JSONException e) {
            context.getLogger().info("Error in Merger: " + e.getMessage());
        }
    }
}