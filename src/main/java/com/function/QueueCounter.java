package com.function;

import java.math.BigDecimal;
import java.util.Base64;
import java.util.Map;

import org.json.JSONException;
import org.json.JSONObject;

import com.azure.core.util.BinaryData;
import com.azure.storage.queue.QueueClient;
import com.azure.storage.queue.QueueClientBuilder;
import com.microsoft.azure.functions.ExecutionContext;
import com.microsoft.azure.functions.annotation.FunctionName;
import com.microsoft.azure.functions.annotation.QueueTrigger;
import com.function.config.AccountConfig;
import com.function.helper.Counter;


/**
 * Listens on the taskqueue. 
 * 
 * Completes task and streams result to mainqueue. Issues merge request after result streaming in mergerequests queue.
 */
public class QueueCounter {

    @FunctionName("QueueCounter")
    public void run(
            @QueueTrigger(name = "msg",
            queueName = AccountConfig.TASKS_QUEUE_NAME,
            connection = "AzureWebJobsStorage") String message,
            final ExecutionContext context) {
        JSONObject jsonObject = new JSONObject(message);

        try {
            int begin = jsonObject.getInt(AccountConfig.AGGREGATION_JOB_RANGE_START);
            int end = jsonObject.getInt(AccountConfig.AGGREGATION_JOB_RANGE_END);
            String resultqueuename = jsonObject.getString(AccountConfig.RESULTS_QUEUE_NAME);
            int aggregationID = jsonObject.getInt(AccountConfig.AGGREGATION_ID);

            QueueClient resultClient = new QueueClientBuilder()
                                .connectionString(AccountConfig.CONNECTION_STRING)
                                .queueName(resultqueuename)
                                .buildClient();
            
            BlobContainerWrapper blobContainerWrapper = new BlobContainerWrapper(jsonObject.getString(AccountConfig.JOB_CONTAINER_PROP));
            BinaryData file = blobContainerWrapper.readFile(jsonObject.getString(AccountConfig.AGGREGATION_JOB_TARGET));

            if (file != null) {
                Map<String, Map<String, BigDecimal>> nationToSumCount = Counter.findCountAndSum(file, begin, end);

                // TODO: logic for assumption, that a single results fits into the 64kB restriction of Azure queues. 
                JSONObject res = new JSONObject()
                            .put(AccountConfig.TYPE_OF_CONTENT, AccountConfig.NEW_COUNT_RESULT)
                            .put(AccountConfig.AGGREGATION_ID, aggregationID)
                            .put(AccountConfig.NEW_COUNT_RESULT, nationToSumCount);

                resultClient.sendMessage(Base64.getEncoder().encodeToString(res.toString().getBytes()));
            } else {
                context.getLogger().info("Something went wrong! We could not read the file!");
            }
        } 
        catch (JSONException e) {
            context.getLogger().info("Error in Merger: " + e.getMessage());
        }
    }
}