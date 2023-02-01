package com.function;

import java.math.BigDecimal;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

import org.javatuples.Pair;
import org.json.JSONException;
import org.json.JSONObject;

import com.azure.core.util.BinaryData;
import com.azure.storage.queue.QueueClient;
import com.azure.storage.queue.QueueClientBuilder;
import com.ctc.wstx.shaded.msv_core.datatype.xsd.datetime.BigDateTimeValueType;
import com.microsoft.azure.functions.ExecutionContext;
import com.microsoft.azure.functions.annotation.FunctionName;
import com.microsoft.azure.functions.annotation.QueueTrigger;
import com.function.config.Config;
import com.function.helper.Counter;;


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
                Map<String, Pair<Double, BigDecimal>> nationToSumCount = Counter.findCountAndSumQueue(file, rangeStart, rangeEnd);

                // TODO: logic for assumption, that a single results fits into the 64kB restriction of Azure queues. 
                JSONObject res = new JSONObject()
                            .put("type", "result")
                            .put("aggregationID", aggregationID)
                            .put("result", nationToSumCount);
                resultClient.sendMessage(Base64.getEncoder().encodeToString(res.toString().getBytes()));
            }
        } 
        catch (JSONException e) {
            context.getLogger().info("Error in Merger: " + e.getMessage());
        }
    }
}