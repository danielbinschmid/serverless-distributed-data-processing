package com.function;


import com.azure.storage.queue.QueueClient;
import com.azure.storage.queue.QueueClientBuilder;
import com.azure.storage.queue.models.QueueMessageItem;

import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.javatuples.Pair;
import org.json.JSONObject;

import com.azure.core.http.rest.PagedIterable;
import com.microsoft.azure.functions.ExecutionContext;
import com.microsoft.azure.functions.OutputBinding;
import com.microsoft.azure.functions.annotation.FunctionName;
import com.microsoft.azure.functions.annotation.QueueOutput;
import com.microsoft.azure.functions.annotation.QueueTrigger;
import com.function.config.Config;


/**
 * Aggregates results from the partitioned tasks of a file input.
 */
public class QueueAggregation {
    private static final int maxQueueMsgs = 2;
    private static final long waitTime = 500;

    private Map<String, Pair<Double, Long>> nationToSumCount;
    private ArrayList<Boolean> aggregationValid;


    @FunctionName("aggregation")
    public void run(
            @QueueTrigger(name = "msg",
                        queueName = Config.AGGREGATION_QUEUE_NAME,
                        connection = "AzureWebJobsStorage") String message,
            @QueueOutput(name= "out",
                        queueName = Config.RESULTS_QUEUE_NAME,
                        connection = "AzureWebJobsStorage") OutputBinding<String> resultMsg,
            final ExecutionContext context) {

        
        nationToSumCount = new HashMap<>();
        aggregationValid = new ArrayList<>();
        JSONObject jsonObject = new JSONObject(message);

        try {
            long first = jsonObject.getLong("firstPartition");
            long last = jsonObject.getLong("lastPartition");

            // init valid list
            for (long i = first; i <= last; i++) aggregationValid.add(false);
            
            // init queue on which the results of the partitioned task come in.
            String partitionResultQueueName = jsonObject.getString("resultqueue");
            QueueClient aggregationResultClient = new QueueClientBuilder()
                                .connectionString(Config.CONNECTION_STRING)
                                .queueName(partitionResultQueueName)
                                .buildClient();

            while(!isAggregationFinished()) {                
                PagedIterable<QueueMessageItem> msgs = aggregationResultClient.receiveMessages(maxQueueMsgs);
    
                for (QueueMessageItem msg: msgs) {
                    
                    byte[] decodedBytes = Base64.getDecoder().decode(msg.getMessageText());
                    String decodedMessage = new String(decodedBytes);
                    
                    JSONObject msgData = new JSONObject(decodedMessage);
                    
                    long id = msgData.getLong("aggregationID");
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
                    aggregationValid.set((int) id, true);
    
                    aggregationResultClient.deleteMessage(msg.getMessageId(), msg.getPopReceipt());
                }

                if (aggregationResultClient.getProperties().getApproximateMessagesCount() == 0) Thread.sleep(waitTime);
            }

            JSONObject result = new JSONObject()
                                    .put("result", nationToSumCount);

            resultMsg.setValue(result.toString());
            
            aggregationResultClient.delete();
        } catch (Exception e) { System.err.println("EXCEPTION: " + e.getMessage()); }    
            
    }

    private boolean isAggregationFinished() {
        boolean isValid = true;
        for(boolean x: aggregationValid) if (!x) isValid = false;
        return isValid;
    }
}