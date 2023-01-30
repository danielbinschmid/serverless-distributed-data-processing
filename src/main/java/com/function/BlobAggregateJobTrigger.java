package com.function;

import com.azure.core.util.BinaryData;
import com.microsoft.azure.functions.ExecutionContext;
import com.microsoft.azure.functions.annotation.*;
import org.json.JSONObject;

import java.util.HashMap;
import java.util.Map;

public class BlobAggregateJobTrigger {
    @FunctionName("BlobAggregateJobTrigger")
    @StorageAccount("BlobConnectionString")
    public void run(
            @BlobTrigger(name = "file",
                    dataType = "binary",
                    path = "aggregationjobs/{name}.{number}.{extension}",
                    connection = "AzureWebJobsStorage") byte[] content,
            @BindingName("name") String filename,
            @BindingName("number") String partitionNumber,
            final ExecutionContext context
    ) {
        StringBuilder fileContent = new StringBuilder();
        for (byte b : content) {
            fileContent.append((char) b);
        }


        JSONObject jsonObject = new JSONObject(fileContent.toString());
        /*
        context.getLogger().info("********************************************");
        context.getLogger().info("Job filename: " + filename + "; Partition number: " + partitionNumber);
        context.getLogger().info("Range start: " + jsonObject.get("rangeStart"));
        context.getLogger().info("Range end: " + jsonObject.get("rangeEnd"));
        context.getLogger().info("Container: " + jsonObject.get("container"));
        context.getLogger().info("Target: " + jsonObject.get("target"));
        context.getLogger().info("********************************************");
        */
        long begin = jsonObject.getLong("rangeStart");
        long end = jsonObject.getLong("rangeEnd");

        BlobContainerWrapper blobContainerWrapper = new BlobContainerWrapper(jsonObject.getString("container"));
        BinaryData file = blobContainerWrapper.readFile(jsonObject.getString("target"));


        if (file == null) {
            context.getLogger().info("Something went wrong! We could not read the file!");
        } else {
            byte[] fileArray = file.toBytes();
            long counter = 0;
            // Skip the lines until we reach the begin line
            for (byte b : fileArray) {

                if (counter >= begin) {
                    break;
                }

                if (b == '\n') {
                    counter++;
                }
            }

            Map<String, Double> nationKeyToAccountBalanceSum = new HashMap<>();
            Map<String, Double> nationKeyToCount = new HashMap<>();
            // Continue to the ones that are in our interest
            StringBuilder temp = new StringBuilder();
            for (byte b : fileArray) {
                if (counter >= end) {
                    break;
                }

                if (b == '\n') {
                    // Insert the current value to our map;
                    try {
                        countCurrent(nationKeyToAccountBalanceSum,nationKeyToCount, temp.toString());
                    } catch (Exception e) {
                        System.err.println(e.getMessage());
                    }
                    counter++;
                    temp = new StringBuilder();
                }

                temp.append((char) b);

            }

            // Sum / occurrences => mean account balance
            Map<String, Double> nationKeyToMean = new HashMap<>();
            nationKeyToAccountBalanceSum.forEach((key, value) -> {
                Double nationKeyCount = nationKeyToCount.get(key);
                Double mean = value / nationKeyCount;
                nationKeyToMean.put(key, mean);
            });

            // We have the values inside a map and we want to write them to another container
            BlobContainerWrapper resultBlobContainerWrapper = new BlobContainerWrapper("aggregationresults");
            JSONObject result = new JSONObject(nationKeyToMean);
            resultBlobContainerWrapper.writeFile(filename + "." + partitionNumber + ".result.json", result.toString());
        }



    }

    private void countCurrent(Map<String, Double> values, Map<String, Double> nationKeyToCount, String line) throws Exception {
        String[] splitArray = line.split("\\|");

        if (splitArray.length != 8) {
            throw new Exception("Wrong input");
        }

        String nationKey = splitArray[3];
        // Count how many entries from this nation there are
        Double nationCount = 0D;
        if (nationKeyToCount.containsKey(nationKey)) {
            nationCount = nationKeyToCount.get(nationKey);
        }
        nationCount++;
        nationKeyToCount.put(nationKey, nationCount);

        String accountBalance = splitArray[5];

        Double curr = 0D;

        if (values.containsKey(nationKey)) {
            curr = values.get(nationKey);
        }

        curr += Double.parseDouble(accountBalance);

        values.put(nationKey, curr);
    }

}

