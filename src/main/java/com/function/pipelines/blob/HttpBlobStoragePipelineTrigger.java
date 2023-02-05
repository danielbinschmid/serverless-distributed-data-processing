package com.function.pipelines.blob;

import java.util.*;

import com.azure.core.util.BinaryData;
import com.function.config.BlobPipelineConfig;
import com.function.config.PipelineConfig;
import com.function.pipelines.helper.Partitioner;
import com.microsoft.azure.functions.annotation.*;
import com.microsoft.azure.functions.*;
import org.json.JSONObject;

/**
 * Azure Functions with HTTP Trigger.
 */
public class HttpBlobStoragePipelineTrigger {
    /**
     * This function listens at endpoint "/api/HttpBlobStoragePiepelienTrigger". Two ways to invoke it using "curl" command in bash:
     * 1. curl -d "HTTP Body" {your host}/api/HttpBlobStoragePiepelienTrigger
     * 2. curl {your host}/api/HttpBlobStoragePiepelienTrigger?name=HTTP%20Query
     */
    @FunctionName("HttpBlobStoragePipelineTrigger")
    public HttpResponseMessage run(
            @HttpTrigger(name = "req", methods = {HttpMethod.GET, HttpMethod.POST}, authLevel = AuthorizationLevel.ANONYMOUS) HttpRequestMessage<Optional<String>> request,
            final ExecutionContext context) {

        BlobContainerWrapper uploadContainer = new BlobContainerWrapper(BlobPipelineConfig.FILE_LIST_CONTAINER_NAME);
        BlobContainerWrapper aggregationJobsContainer = new BlobContainerWrapper(BlobPipelineConfig.AGGREGATION_JOBS_CONTAINER_NAME);

        List<String> listOfSkippedFiles = new ArrayList<>();

        uploadContainer.getAllBlobs().forEach(blob -> {
            BinaryData binaryData = uploadContainer.readFile(blob.getName());
            if (binaryData == null) {
                // TODO: Is skipping the file a good approach?
                System.err.println("Something went wrong while trying to read: " + blob.getName());
                listOfSkippedFiles.add(blob.getName());
                return;
            }

            String filenameForUpload = blob.getName().substring(0, blob.getName().length() - 4);

            for (int i = 0; i < PipelineConfig.N_PARTITIONS; i++) {
                JSONObject jsonObject = Partitioner.getIthPartition(i, binaryData.toBytes(), blob.getName());
                jsonObject.put(PipelineConfig.JOB_CONTAINER_PROP, BlobPipelineConfig.FILE_LIST_CONTAINER_NAME);
                aggregationJobsContainer.writeFile(filenameForUpload + "." + i + ".json", jsonObject.toString());
            }
        });

        StringBuilder response = new StringBuilder();
        response.append("Pipeline was successfully trigger\n")
                .append("List of skipped files: ")
                .append(listOfSkippedFiles);

        return request.createResponseBuilder(HttpStatus.OK).body(response).build();
    }
}
