package com.function.pipelines.blob;

import java.math.BigDecimal;
import java.util.*;

import com.azure.core.util.BinaryData;
import com.function.config.BlobPipelineConfig;
import com.function.pipelines.helper.BlobContainerWrapper;
import com.function.pipelines.helper.Merger;
import com.microsoft.azure.functions.annotation.*;
import com.microsoft.azure.functions.*;

/**
 * Azure Functions with HTTP Trigger.
 */
public class DisplayMergeResult {
    /**
     * This function listens at endpoint "/api/DisplayMergeResult". Two ways to invoke it using "curl" command in bash:
     * 1. curl -d "HTTP Body" {your host}/api/DisplayMergeResult
     * 2. curl {your host}/api/DisplayMergeResult?name=HTTP%20Query
     */
    @FunctionName("DisplayMergeResult")
    public HttpResponseMessage run(
            @HttpTrigger(name = "req", methods = {HttpMethod.GET, HttpMethod.POST}, authLevel = AuthorizationLevel.ANONYMOUS) HttpRequestMessage<Optional<String>> request,
            final ExecutionContext context) {
        context.getLogger().info("Java HTTP trigger processed a request.");

        BlobContainerWrapper mergeContainer = new BlobContainerWrapper(BlobPipelineConfig.MERGE_RESULT_CONTAINER_NAME);
        BinaryData resultBinaryData = mergeContainer.readFile(BlobPipelineConfig.MERGE_RESULT_FILENAME);
        if (resultBinaryData == null) {
            return request.createResponseBuilder(HttpStatus.BAD_REQUEST).body("There is no result yet").build();
        }

        try {

            Map<String, BigDecimal> result = Merger.calculateMeanAccountBalance(resultBinaryData);

            return request.createResponseBuilder(HttpStatus.OK).body(result).build();
        } catch (Exception e) {
            System.err.println(e.getMessage());
            return request.createResponseBuilder(HttpStatus.INTERNAL_SERVER_ERROR).body("Something went wrong!").build();
        }
    }
}
