package com.function;

import com.azure.core.util.BinaryData;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.microsoft.azure.functions.annotation.BlobTrigger;
import com.microsoft.azure.functions.annotation.FunctionName;
import com.microsoft.azure.functions.annotation.StorageAccount;
import org.json.JSONObject;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;

public class BlobMergeAggregateTrigger {
    @FunctionName("BlobMergeAggregateTrigger")
    @StorageAccount("BlobConnectionString")
    public void run(
            @BlobTrigger(name = "file",
                    dataType = "binary",
                    // customer.00.1.result.json
                    path = "mergingjobs/{prefixname}.{ext}",
                    connection = "AzureWebJobsStorage") byte[] content
    ) {
        StringBuilder fileContent = new StringBuilder();
        for (byte b : content) {
            fileContent.append((char) b);
        }
        JSONObject jsonObject = new JSONObject(fileContent.toString());

        String filename = jsonObject.getString("prefixname");
        String container = jsonObject.getString("container");

        if (filename == null || container == null) {
            System.err.println("Something went wrong. The job file couldn't be parsed.");
            return;
        }

        BlobContainerWrapper readContainer = new BlobContainerWrapper(container);

        Map<String, Map<String, BigDecimal>> nationKeyToSumAndCount = new HashMap<>();

        readContainer.getAllBlobs().forEach(blob -> {
            // Read all blobs that have this prefix
            if (blob.getName().contains(filename)) {
                BinaryData binaryData = readContainer.readFile(blob.getName());
                try {
                    Map<String, Map<String, BigDecimal>> jsonMap = new ObjectMapper().readValue(binaryData.toString(), new TypeReference<Map<String, Map<String, BigDecimal>>>() {
                    });
                    jsonMap.forEach((nationKey, nestedMap) -> {
                        Map<String, BigDecimal> mergeStateMap = nationKeyToSumAndCount.get(nationKey);
                        if (mergeStateMap == null) {
                            mergeStateMap = new HashMap<>();
                        }

                        // Java needed this wtf !?
                        Map<String, BigDecimal> finalMergeStateMap = mergeStateMap;


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

                } catch (JsonProcessingException e) {
                    throw new RuntimeException(e);
                }

            }
        });


        // We should be ready and everything should be merged
        BlobContainerWrapper writeContainer = new BlobContainerWrapper("mergeresult");
        // Check if there is a result already. If that's the case merge the results.
        BinaryData mergeResultBinary = writeContainer.readFile("merge-result.json");
        if (mergeResultBinary == null) {
            // If that's the first result, just dump the data
            JSONObject result = new JSONObject(nationKeyToSumAndCount);
            writeContainer.writeFile( "merge-result.json", result.toString());
        } else {
            // We need to merge otherwise ^_^
            try {
                Map<String, Map<String, BigDecimal>> mergeResultMap = new ObjectMapper().readValue(mergeResultBinary.toString(), new TypeReference<Map<String, Map<String, BigDecimal>>>() {
                });

                // Go trough each nation key of the merge result map that's saved
                mergeResultMap.forEach((nationKey, nestedMap) -> {
                    // Get the nested map for the current nation key
                    Map<String, BigDecimal> mergeStateMap = nationKeyToSumAndCount.get(nationKey);
                    // If we still don't have such an entry of a nested map, create one
                    if (mergeStateMap == null) {
                        mergeStateMap = new HashMap<>();
                    }

                    // Java needed this wtf !?
                    Map<String, BigDecimal> finalMergeStateMap = mergeStateMap;

                    // For both count and sum, aggregate the result by summing them up
                    nestedMap.forEach((key, value) -> {

                        if (finalMergeStateMap.containsKey(key)) {
                            BigDecimal bigDecimal = finalMergeStateMap.get(key);
                            finalMergeStateMap.put(key, value.add(bigDecimal)); // Count and Sum should be summed up
                        } else {
                            finalMergeStateMap.put(key, value);
                        }

                    });

                    // Update the entry for this nation key
                    nationKeyToSumAndCount.put(nationKey, mergeStateMap);
                });



                // We are done, so simply overwrite the file
                JSONObject result = new JSONObject(nationKeyToSumAndCount);
                writeContainer.writeFile( "merge-result.json", result.toString());
            } catch (Exception e) {
                System.err.println("Something went wrong during the reading of the merge-result and the task execution");
            }
        }

    }
}

