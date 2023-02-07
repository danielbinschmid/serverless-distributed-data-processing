package com.function.pipelines.queue;


import com.azure.storage.queue.QueueClient;
import com.azure.storage.queue.QueueClientBuilder;
import com.azure.storage.queue.models.QueueMessageItem;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

import org.json.JSONArray;
import org.json.JSONObject;

import com.azure.core.http.rest.PagedIterable;
import com.microsoft.azure.functions.ExecutionContext;
import com.microsoft.azure.functions.OutputBinding;
import com.microsoft.azure.functions.annotation.FunctionName;
import com.microsoft.azure.functions.annotation.QueueOutput;
import com.microsoft.azure.functions.annotation.QueueTrigger;

import com.function.config.QueuePipelineConfig;
import com.function.config.PipelineConfig;
import com.function.pipelines.helper.Counter;
import com.function.config.AccountConfig;


/**
 * Aggregates results from the partitioned tasks of a file input.
 */
public class QueueAggregation {
    private static final int maxQueueMsgs = 5;
    private static final long waitTime = 500;

    private Map<String, Map<String, BigDecimal>> nationKeyToSumAndCount;
    private Map<String, Boolean> aggregationValid;


    @FunctionName("QueueAggregation")
    public void run(
            @QueueTrigger(name = "msg",
                          queueName = QueuePipelineConfig.AGGREGATION_QUEUE_NAME,
                          connection = "AzureWebJobsStorage") String message,
            @QueueOutput(name= "out",
                         queueName = QueuePipelineConfig.RESULTS_QUEUE_NAME,
                         connection = "AzureWebJobsStorage") OutputBinding<String> resultMsg,
            final ExecutionContext context) {
        
        nationKeyToSumAndCount = new HashMap<>();
        aggregationValid = new HashMap<>();
        JSONObject jsonObject = new JSONObject(message);

        try {
            JSONArray partitions = jsonObject.getJSONArray(QueuePipelineConfig.PARTITIONS);

            for (int i = 0; i < partitions.length(); i++) {
                String partition = partitions.getString(i);
                aggregationValid.put(partition, false);
            }

            // init queue on which the results of the partitioned task come in.
            String partitionResultQueueName = jsonObject.getString(QueuePipelineConfig.RESULTS_QUEUE_NAME);
            QueueClient aggregationResultClient = new QueueClientBuilder()
                                .connectionString(AccountConfig.CONNECTION_STRING)
                                .queueName(partitionResultQueueName)
                                .buildClient();

            while(!isAggregationFinished()) {                
                PagedIterable<QueueMessageItem> msgs = aggregationResultClient.receiveMessages(maxQueueMsgs);
    
                for (QueueMessageItem msg: msgs) {
                    
                    byte[] decodedBytes = Base64.getDecoder().decode(msg.getMessageText());
                    String decodedMessage = new String(decodedBytes);
                    
                    JSONObject msgData = new JSONObject(decodedMessage);
                    String id = msgData.getString(PipelineConfig.AGGREGATION_ID);
                    
                    JSONObject newCounts = msgData.getJSONObject(QueuePipelineConfig.NEW_COUNT_RESULT);

                    JSONObject nationKeyToAccountBalanceSum = newCounts.getJSONObject(Counter.NATION_KEY_TO_ACCOUNT_BALANCE_SUM_MAP);
                    JSONObject nationKeyToCount = newCounts.getJSONObject(Counter.NATION_KEY_TO_COUNT_MAP);

                    for (String nationKey: nationKeyToAccountBalanceSum.keySet()) {
                        BigDecimal nationSum = nationKeyToAccountBalanceSum.getBigDecimal(nationKey);
                        BigDecimal nationCount = nationKeyToCount.getBigDecimal(nationKey);

                        // update current
                        if (nationKeyToSumAndCount.containsKey(nationKey)) {
                            nationSum = nationSum.add(nationKeyToSumAndCount.get(nationKey).get("sum"));
                            nationCount = nationCount.add(nationKeyToSumAndCount.get(nationKey).get("count"));
                        }

                        Map<String, BigDecimal> nestedMapwithSumAndCount = new HashMap<>();
                        nestedMapwithSumAndCount.put(PipelineConfig.MERGE_RESULT_SUM, nationSum);
                        nestedMapwithSumAndCount.put(PipelineConfig.MERGE_RESULT_COUNT, nationCount);
                        
                        nationKeyToSumAndCount.put(nationKey, nestedMapwithSumAndCount);
                    }
                    
                    aggregationValid.put(id, true);
                    context.getLogger().info("AGGREGATION: New aggregation result: " + id);
                    // context.getLogger().info("AGGREGATION: Missing aggregations: " + getMissingAggregations());
                    aggregationResultClient.deleteMessage(msg.getMessageId(), msg.getPopReceipt());
                }

                if (aggregationResultClient.getProperties().getApproximateMessagesCount() == 0) Thread.sleep(waitTime);
            }

            JSONObject result = new JSONObject()
                                    .put(QueuePipelineConfig.NEW_COUNT_RESULT, nationKeyToSumAndCount);

            resultMsg.setValue(result.toString());
            
            aggregationResultClient.delete();
        } catch (Exception e) { 
            System.err.println("EXCEPTION in Aggregation: " + e.getMessage()); 
        }    
            
    }

    private ArrayList<String> getMissingAggregations() {
        ArrayList<String> missing = new ArrayList<>();
        for (Map.Entry<String, Boolean> entry: aggregationValid.entrySet()) {
            if (!entry.getValue()) missing.add(entry.getKey()); 
        }
        return missing;
    }

    private boolean isAggregationFinished() {
        boolean isValid = true;
        for(boolean x: aggregationValid.values()) if (!x) isValid = false;
        return isValid;
    }
}