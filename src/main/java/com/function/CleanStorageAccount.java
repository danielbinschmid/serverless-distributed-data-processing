package com.function;

import com.function.config.BlobPipelineConfig;
import com.function.pipelines.helper.BlobContainerWrapper;

public class CleanStorageAccount {
    public static void main(String[] args) {
        BlobContainerWrapper aggregationJobs = new BlobContainerWrapper(BlobPipelineConfig.AGGREGATION_JOBS_CONTAINER_NAME);
        aggregationJobs.getAllBlobs().forEach(blob -> aggregationJobs.getBlobClient(blob.getName()).delete());

        BlobContainerWrapper aggregationResults = new BlobContainerWrapper(BlobPipelineConfig.AGGREGATION_RESULTS_CONTAINER_NAME);
        aggregationResults.getAllBlobs().forEach(blob -> aggregationResults.getBlobClient(blob.getName()).delete());

        BlobContainerWrapper mergingJobs = new BlobContainerWrapper(BlobPipelineConfig.MERGING_JOBS_CONTAINER_NAME);
        mergingJobs.getAllBlobs().forEach(blob -> mergingJobs.getBlobClient(blob.getName()).delete());

        BlobContainerWrapper mergingResults = new BlobContainerWrapper(BlobPipelineConfig.MERGE_RESULT_CONTAINER_NAME);
        mergingResults.getAllBlobs().forEach(blob -> mergingResults.getBlobClient(blob.getName()).delete());
    }

}
