package com.function.config;

public class BlobPipelineConfig {
    // ----------------------------- Storage account container names. ---------------------------------
    

    // Names of the containers where the aggregation jobs descriptions are uploaded
    public final static String AGGREGATION_JOBS_CONTAINER_NAME = "aggregationjobs";
    // Names of the containers where the aggregation results are uploaded
    public final static String AGGREGATION_RESULTS_CONTAINER_NAME = "aggregationresults";

    // Names of the containers where the merging jobs descriptions are uploaded
    public final static String MERGING_JOBS_CONTAINER_NAME = "mergingjobs";
    // Names of the containers where the merge result is uploaded
    public final static String MERGE_RESULT_CONTAINER_NAME = "mergeresult";


    // -------------------------------- Filenames used for writing. -----------------------------------
    public static final String MERGE_RESULT_FILENAME = "merge-result.json";
}
