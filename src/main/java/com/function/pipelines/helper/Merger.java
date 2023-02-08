package com.function.pipelines.helper;

import com.azure.core.util.BinaryData;
import com.azure.storage.blob.specialized.BlobLeaseClient;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.function.config.PipelineConfig;
import com.microsoft.azure.functions.ExecutionContext;


import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

public class Merger {

    public static void mergeResults(Map<String, Map<String, BigDecimal>> nationKeyToSumAndCount, BinaryData binaryData) throws JsonProcessingException {
        Map<String, Map<String, BigDecimal>> jsonMap = new ObjectMapper().readValue(binaryData.toString(), new TypeReference<Map<String, Map<String, BigDecimal>>>() {
        });
        jsonMap.forEach((nationKey, nestedMap) -> {
            Map<String, BigDecimal> mergeStateMap = nationKeyToSumAndCount.get(nationKey);
            if (mergeStateMap == null) {
                mergeStateMap = new HashMap<>();
            }

            // Java needed this wtf !?
            Map<String, BigDecimal> finalMergeStateMap = mergeStateMap;

            // In both count and sum cases we aggregate by summing up
            nestedMap.forEach((key, value) -> {
                if (finalMergeStateMap.containsKey(key)) {
                    BigDecimal bigDecimal = finalMergeStateMap.get(key);
                    finalMergeStateMap.put(key, value.add(bigDecimal)); // Count and Sum should be summed up
                } else {
                    finalMergeStateMap.put(key, value);
                }

            });

            nationKeyToSumAndCount.put(nationKey, mergeStateMap);
        });
    }

    public static Map<String, BigDecimal> calculateMeanAccountBalance(BinaryData resultBinaryData) throws JsonProcessingException {
        Map<String, Map<String, BigDecimal>> jsonMap = new ObjectMapper().readValue(resultBinaryData.toString(), new TypeReference<Map<String, Map<String, BigDecimal>>>() {});

        Map<String, BigDecimal> nationKeyToMeanAccountBalance = new HashMap<>();

        jsonMap.forEach((nationKey, nestedMap) -> {
            AtomicReference<BigDecimal> sum = new AtomicReference<>();
            AtomicReference<BigDecimal> count = new AtomicReference<>();


            nestedMap.forEach((key, value) -> {
                // 2 cases only
                if (key.equals(PipelineConfig.MERGE_RESULT_COUNT)) {
                    count.set(value);
                } else {
                    sum.set(value);
                }
            });

            BigDecimal meanAccountBalance = sum.get().divide(count.get(), 2, RoundingMode.HALF_UP);
            nationKeyToMeanAccountBalance.put(nationKey, meanAccountBalance);
        });

        return nationKeyToMeanAccountBalance;
    }

    /**
     * Recursive lease attempt.
     *
     * @param leaseClient
     * @param context
     * @return
     */
    public static String acquireLease(BlobLeaseClient leaseClient, ExecutionContext context) {
        try
        {
            String ret = leaseClient.acquireLease(20);
            context.getLogger().info("Leased shared result blob.");
            return ret;
        }
        catch (Exception e)
        {
            context.getLogger().info("blob can not be leased. Trying again. " + e.getMessage());
            try {
                Thread.sleep((int) (Math.random() * 500 + 100));
                return acquireLease(leaseClient, context);
            } catch (Exception e2) {
                System.err.println(e2.getMessage());
            }
        }
        return "";
    }
}
