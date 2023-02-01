package com.function;

import com.azure.core.util.BinaryData;
import com.function.config.Config;
import com.function.helper.Counter;
import com.microsoft.azure.functions.ExecutionContext;
import com.microsoft.azure.functions.annotation.*;
import org.json.JSONObject;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;

public class BlobAggregateJobTrigger {
    @FunctionName("BlobAggregateJobTrigger")
    public void run(
            @BlobTrigger(name = "file",
                    dataType = "binary",
                    path = Config.AGGREGATION_JOBS_CONTAINER_NAME + "/{name}.{number}.{extension}",
                    connection = "AzureWebJobsStorage") byte[] content,
            @BindingName("name") String filename,
            @BindingName("number") String partitionNumber,
            final ExecutionContext context
    ) {
        //TODO: Fix strange exception !?
        /*
        [2023-01-31T19:00:25.436Z] Trigger Details: MessageId: 68c9c118-5fae-4a12-b1a2-66178a3259c5, DequeueCount: 1, InsertedOn: 2023-01-31T19:00:25.000+00:00, BlobCreated: 2023-01-31T19:00:16.000+00:00, BlobLastModified: 2023-01-31T19:00:16.000+00:00
        [2023-01-31T19:00:25.733Z] EXCEPTION: Status code 404, "﻿<?xml version="1.0" encoding="utf-8"?><Error><Code>BlobNotFound</Code><Message>The specified blob does not exist.
        [2023-01-31T19:00:25.733Z] RequestId:b5dd531b-d01e-00a0-2fa6-3565cc000000
         */
        StringBuilder fileContent = new StringBuilder();
        for (byte b : content) {
            fileContent.append((char) b);
        }
        JSONObject jsonObject = new JSONObject(fileContent.toString());
        int begin = jsonObject.getInt(Config.AGGREGATION_JOB_RANGE_START);
        int end = jsonObject.getInt(Config.AGGREGATION_JOB_RANGE_END);

        BlobContainerWrapper blobContainerWrapper = new BlobContainerWrapper(jsonObject.getString(Config.JOB_CONTAINER_PROP));
        BinaryData file = blobContainerWrapper.readFile(jsonObject.getString(Config.AGGREGATION_JOB_TARGET));


        if (file == null) {
            context.getLogger().info("Something went wrong! We could not read the file!");
        } else {
            // Count the occurrences and sum the account balance per nation key
            Map<Integer, Map<String, BigDecimal>> countedResults = Counter.findCountAndSumBlob(file, begin, end);
            // Extract both of the maps
            Map<String, BigDecimal> nationKeyToAccountBalanceSum = countedResults.get(Counter.NATION_KEY_TO_ACCOUNT_BALANCE_SUM_MAP);
            Map<String, BigDecimal> nationKeyToCount = countedResults.get(Counter.NATION_KEY_TO_COUNT_MAP);

            // Create result map and populate it
            Map<String, Map<String, BigDecimal>> nationKeyToSumAndCount = new HashMap<>();
            nationKeyToAccountBalanceSum.forEach((nationKey, sum) -> {
                BigDecimal nationKeyCount = nationKeyToCount.get(nationKey);

                Map<String, BigDecimal> nestedMapWithSumAndCount = new HashMap<>();
                nestedMapWithSumAndCount.put(Config.MERGE_RESULT_SUM, sum);
                nestedMapWithSumAndCount.put(Config.MERGE_RESULT_COUNT, nationKeyCount);

                nationKeyToSumAndCount.put(nationKey, nestedMapWithSumAndCount);
            });

            // We have the values inside a map and we want to write them to another container
            BlobContainerWrapper resultBlobContainerWrapper = new BlobContainerWrapper(Config.AGGREGATION_RESULTS_CONTAINER_NAME);
            JSONObject result = new JSONObject(nationKeyToSumAndCount);
            resultBlobContainerWrapper.writeFile(filename + "." + partitionNumber + ".result.json", result.toString());

            boolean shouldStartMerging = resultBlobContainerWrapper.shouldStartMerging(filename, Config.N_PARTITIONS);

            // All 10 files have been written and the merging can start
            if (shouldStartMerging) {
                JSONObject mergeJobJson = new JSONObject();
                mergeJobJson.put(Config.MERGING_JOB_PREFIX, filename);
                mergeJobJson.put(Config.JOB_CONTAINER_PROP, Config.AGGREGATION_RESULTS_CONTAINER_NAME);

                BlobContainerWrapper mergeJobsContainerWrapper = new BlobContainerWrapper(Config.MERGING_JOBS_CONTAINER_NAME);
                mergeJobsContainerWrapper.writeFile(filename + ".json", mergeJobJson.toString());
            }

        }

    }

}
