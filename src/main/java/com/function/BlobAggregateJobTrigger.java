package com.function;

import com.azure.core.util.BinaryData;
import com.microsoft.azure.functions.ExecutionContext;
import com.microsoft.azure.functions.annotation.*;
import org.json.JSONObject;

import java.math.BigDecimal;
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
            int i = 0;
            for (; i < fileArray.length; i++) {
                if (fileArray[i] == '\n') {
                    counter++;
                }
                if (counter == begin) {
                    break;
                }
            }

            Map<String, BigDecimal> nationKeyToAccountBalanceSum = new HashMap<>();
            Map<String, BigDecimal> nationKeyToCount = new HashMap<>();
            // Continue to the ones that are in our interest
            StringBuilder temp = new StringBuilder();
            for (; i < fileArray.length; i++) {
                if (counter >= end) {
                    break;
                }

                if (fileArray[i] == '\n') {
                    if (temp.length() != 0) {
                        try {
                            countCurrent(nationKeyToAccountBalanceSum, nationKeyToCount, temp.toString());
                        } catch (Exception e) {
                            System.err.println(e.getMessage());
                        }
                    }
                    // Insert the current value to our map;
                    counter++;
                    temp = new StringBuilder();
                    continue;
                }

                temp.append((char) fileArray[i]);
            }

            if (temp.length() != 0) {
                try {
                    countCurrent(nationKeyToAccountBalanceSum, nationKeyToCount, temp.toString());
                } catch (Exception e) {
                    System.err.println(e.getMessage());
                }
            }

            /*

            {
               nation key ->
                "0": {
                    count: number
                    sum: number
                },
                ...
            }

             */
            Map<String, Map<String, BigDecimal>> nationKeyToSumAndCount = new HashMap<>();
            nationKeyToAccountBalanceSum.forEach((nationKey, sum) -> {
                BigDecimal nationKeyCount = nationKeyToCount.get(nationKey);

                Map<String, BigDecimal> nestedMapWithSumAndCount = new HashMap<>();
                nestedMapWithSumAndCount.put("sum", sum);
                nestedMapWithSumAndCount.put("count", nationKeyCount);

                nationKeyToSumAndCount.put(nationKey, nestedMapWithSumAndCount);
            });

            // We have the values inside a map and we want to write them to another container
            BlobContainerWrapper resultBlobContainerWrapper = new BlobContainerWrapper("aggregationresults");
            JSONObject result = new JSONObject(nationKeyToSumAndCount);
            resultBlobContainerWrapper.writeFile(filename + "." + partitionNumber + ".result.json", result.toString());

            boolean shouldStartMerging = resultBlobContainerWrapper.shouldStartMerging(filename, 10);

            // All 10 files have been written and the merging can start
            if (shouldStartMerging) {
                JSONObject mergeJobJson = new JSONObject();
                mergeJobJson.put("prefixname", filename);
                mergeJobJson.put("container", "aggregationresults");

                BlobContainerWrapper mergeJobsContainerWrapper = new BlobContainerWrapper("mergingjobs");
                mergeJobsContainerWrapper.writeFile(filename + ".json", mergeJobJson.toString());
            }

        }


    }

    private void countCurrent(Map<String, BigDecimal> values, Map<String, BigDecimal> nationKeyToCount, String line) throws Exception {
        String[] splitArray = line.split("\\|");

        if (splitArray.length != 8) {
            throw new Exception("Wrong input");
        }

        String nationKey = splitArray[3];

        // Count how many entries from this nation there are
        BigDecimal nationCount = new BigDecimal("0");
        if (nationKeyToCount.containsKey(nationKey)) {
            nationCount = nationKeyToCount.get(nationKey);
        }
        nationCount = nationCount.add(new BigDecimal("1"));

        nationKeyToCount.put(nationKey, nationCount);

        String accountBalance = splitArray[5];

        BigDecimal curr = new BigDecimal("0.0");
        if (values.containsKey(nationKey)) {
            curr = values.get(nationKey);
        }

        curr = curr.add(new BigDecimal(accountBalance));

        values.put(nationKey, curr);
    }

}
