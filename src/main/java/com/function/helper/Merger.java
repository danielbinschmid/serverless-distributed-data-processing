package com.function.helper;

import com.azure.core.util.BinaryData;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;

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
}
