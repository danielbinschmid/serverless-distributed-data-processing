package com.function;


import com.azure.storage.queue.QueueClient;
import com.azure.storage.queue.QueueClientBuilder;
import com.azure.storage.queue.models.QueueMessageItem;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

import org.json.JSONObject;

import com.azure.core.http.rest.PagedIterable;
import com.microsoft.azure.functions.ExecutionContext;
import com.microsoft.azure.functions.OutputBinding;
import com.microsoft.azure.functions.annotation.FunctionName;
import com.microsoft.azure.functions.annotation.QueueOutput;
import com.microsoft.azure.functions.annotation.QueueTrigger;

import com.function.config.Config;
import com.function.config.AccountConfig;
import com.function.helper.Counter;


/**
 * Aggregates results from the partitioned tasks of a file input.
 */
public class QueueAggregation {
    private static final int maxQueueMsgs = 2;
    private static final long waitTime = 500;

    private Map<String, Map<String, BigDecimal>> nationKeyToSumAndCount;
    private ArrayList<Boolean> aggregationValid;


    @FunctionName("QueueAggregation")
    public void run(
            @QueueTrigger(name = "msg",
                          queueName = Config.AGGREGATION_QUEUE_NAME,
                          connection = "AzureWebJobsStorage") String message,
            @QueueOutput(name= "out",
                         queueName = Config.RESULTS_QUEUE_NAME,
                         connection = "AzureWebJobsStorage") OutputBinding<String> resultMsg,
            final ExecutionContext context) {
        
        nationKeyToSumAndCount = new HashMap<>();
        aggregationValid = new ArrayList<>();
        JSONObject jsonObject = new JSONObject(message);

        try {
            int first = jsonObject.getInt(Config.FIRST_PARTITION);
            int last = jsonObject.getInt(Config.LAST_PARTITION);

            // init valid list
            for (long i = first; i <= last; i++) aggregationValid.add(false);
            
            // init queue on which the results of the partitioned task come in.
            String partitionResultQueueName = jsonObject.getString(Config.RESULTS_QUEUE_NAME);
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
                    long id = msgData.getLong(Config.AGGREGATION_ID);
                    JSONObject newCounts = msgData.getJSONObject(Config.NEW_COUNT_RESULT);

                    JSONObject nationKeyToAccountBalanceSum = newCounts.getJSONObject(Counter.NATION_KEY_TO_ACCOUNT_BALANCE_SUM_MAP);
                    JSONObject nationKeyToCount = newCounts.getJSONObject(Counter.NATION_KEY_TO_COUNT_MAP);

                    for (String nationKey: nationKeyToAccountBalanceSum.keySet()) {
                        BigDecimal nationSum = nationKeyToAccountBalanceSum.getBigDecimal(nationKey);
                        BigDecimal nationCount = nationKeyToCount.getBigDecimal(nationKey);

                        Map<String, BigDecimal> nestedMapwithSumAndCount = new HashMap<>();
                        nestedMapwithSumAndCount.put(Config.MERGE_RESULT_SUM, nationSum);
                        nestedMapwithSumAndCount.put(Config.MERGE_RESULT_COUNT, nationCount);
                        
                        nationKeyToSumAndCount.put(nationKey, nestedMapwithSumAndCount);
                    }
                    
                    aggregationValid.set((int) id, true);
                    aggregationResultClient.deleteMessage(msg.getMessageId(), msg.getPopReceipt());
                }

                if (aggregationResultClient.getProperties().getApproximateMessagesCount() == 0) Thread.sleep(waitTime);
            }

            JSONObject result = new JSONObject()
                                    .put(Config.NEW_COUNT_RESULT, nationKeyToSumAndCount);

            resultMsg.setValue(result.toString());
            
            aggregationResultClient.delete();
        } catch (Exception e) { 
            System.err.println("EXCEPTION: " + e.getMessage()); 
        }    
            
    }

    private boolean isAggregationFinished() {
        boolean isValid = true;
        for(boolean x: aggregationValid) if (!x) isValid = false;
        return isValid;
    }
}