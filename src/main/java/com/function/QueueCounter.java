package com.function;

import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

import org.javatuples.Pair;
import org.json.JSONException;
import org.json.JSONObject;

import com.azure.core.util.BinaryData;
import com.azure.storage.queue.QueueClient;
import com.azure.storage.queue.QueueClientBuilder;
import com.microsoft.azure.functions.ExecutionContext;
import com.microsoft.azure.functions.annotation.FunctionName;
import com.microsoft.azure.functions.annotation.QueueTrigger;
import com.function.config.Config;


/**
 * Listens on the taskqueue. 
 * 
 * Completes task and streams result to mainqueue. Issues merge request after result streaming in mergerequests queue.
 */
public class QueueCounter {

    @FunctionName("counter")
    public void run(
            @QueueTrigger(name = "msg",
            queueName = Config.TASKS_QUEUE_NAME,
            connection = "AzureWebJobsStorage") String message,
            final ExecutionContext context) {
        JSONObject jsonObject = new JSONObject(message);
            

        try {
            long rangeStart = jsonObject.getLong("rangeStart");
            long rangeEnd = jsonObject.getLong("rangeEnd");
            String resultqueuename = jsonObject.getString("resultqueueName");
            int aggregationID = jsonObject.getInt("aggregationID");

            QueueClient resultClient = new QueueClientBuilder()
                                .connectionString(Config.CONNECTION_STRING)
                                .queueName(resultqueuename)
                                .buildClient();
            
            BlobContainerWrapper blobContainerWrapper = new BlobContainerWrapper(jsonObject.getString("container"));
            BinaryData file = blobContainerWrapper.readFile(jsonObject.getString("target"));


            if (file == null) {
                context.getLogger().info("Something went wrong! We could not read the file!");
            } else {
                byte[] fileArray = file.toBytes();
                long counter = 0;

                // Skip the lines until we reach the begin line
                int i = 0;
                for (; i < fileArray.length; i++) {
                    if (counter == rangeStart) break;
                    if (fileArray[i] == '\n') counter++;
                }

                Map<String, Pair<Double, Long>> nationToSumCount = new HashMap<>();
                // Continue to the ones that are in our interest
                StringBuilder temp = new StringBuilder();
                for (; i < fileArray.length; i++) {
                    if (counter > rangeEnd) {
                        break;
                    }
    

                    if (fileArray[i] == '\n') {
                        // Insert the current value to our map;
                        if (temp.length() != 0) {
                            try {
                                countCurrent(nationToSumCount, temp.toString());
                            } catch (Exception e) {
                                System.err.println(e.getMessage());
                            }
                        }
                        counter++;
                        temp = new StringBuilder();
                        continue;
                    }
                    temp.append((char) fileArray[i]);
                }   

                if (temp.length() != 0) {
                    try {
                        countCurrent(nationToSumCount, temp.toString());
                    } catch (Exception e) {
                        System.err.println(e.getMessage());
                    }
                }
    

                // TODO: logic for assumption, that a single results fits into the 64kB restriction of Azure queues. 
                JSONObject res = new JSONObject();
                res.put("type", "result");
                res.put("aggregationID", aggregationID);
                res.put("result", nationToSumCount);
                resultClient.sendMessage(Base64.getEncoder().encodeToString(res.toString().getBytes()));
            }
        } 
        catch (JSONException e) {
            context.getLogger().info("Error in Merger: " + e.getMessage());
        }
        
    }


    private void countCurrent(Map<String, Pair<Double, Long>> nationToSumCount, String line) throws Exception {
        String[] splitArray = line.split("\\|");

        if (splitArray.length != 8) {
            throw new Exception("Wrong input");
        }

        String nationKey = splitArray[3];
        // Count how many entries from this nation there are
        long nationCount = 0l;
        double curr = 0D;
        String accountBalance = splitArray[5];
        if (nationToSumCount.containsKey(nationKey)) {
            nationCount = nationToSumCount.get(nationKey).getValue1();
            curr = nationToSumCount.get(nationKey).getValue0();
        }
        nationCount ++;
        curr += Double.parseDouble(accountBalance);
        nationToSumCount.put(nationKey, new Pair<Double,Long>(curr, nationCount));

    }
}