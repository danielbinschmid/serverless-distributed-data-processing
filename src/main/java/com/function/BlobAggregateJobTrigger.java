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
        StringBuilder fileContent = new StringBuilder();
        for (byte b : content) {
            fileContent.append((char) b);
        }

        JSONObject jsonObject = new JSONObject(fileContent.toString());
        int begin = jsonObject.getInt(Config.AGGREGATION_JOB_RANGE_START);
        int end = jsonObject.getInt(Config.AGGREGATION_JOB_RANGE_END);

        BlobContainerWrapper blobContainerWrapper = new BlobContainerWrapper(jsonObject.getString(Config.JOB_CONTAINER_PROP));
        BinaryData file = blobContainerWrapper.readFile(jsonObject.getString(Config.AGGREGATION_JOB_TARGET));

        if (file != null) {
            // Count the occurrences and sum the account balance per nation key
            Map<String, Map<String, BigDecimal>> countedResults = Counter.findCountAndSum(file, begin, end);

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
        } else {
            context.getLogger().info("Something went wrong! We could not read the file!");
        }

    }

}
