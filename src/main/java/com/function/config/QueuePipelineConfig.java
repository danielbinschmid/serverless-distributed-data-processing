package com.function.config;

public class QueuePipelineConfig {
    // -------------------------------- Azure storage queue names -------------------------------------
    // Need to be created *before* deploying the pipeline.
    
    // Queue for aggregation tasks
    public static final String AGGREGATION_QUEUE_NAME = "aggregationqueue";
    // Queue to push in results for file inputs
    public static final String RESULTS_QUEUE_NAME = "resultsqueue";
    // Queue to append parallelized tasks
    public static final String TASKS_QUEUE_NAME = "tasksqueue";

    // ------------------------------------------------------------------------------------------------

    // -------------------------------- Azure blob container names ------------------------------------
    // Need to be created *before* deploying the pipeline.

    // Blob container which contains the results of the pipeline
    public static final String RESULTS_BLOB_CONTAINER = "results";

    // ------------------------------------------------------------------------------------------------

    // filename of result state blob. 
    public static final String FINAL_RESULTS_STATE_BLOB_NAME = "results.json";

    // filename of result output blob. 
    public static final String FINAL_RESULTS_OUTPUT_BLOB_NAME = "results_avgs.json";



    public static final String FIRST_PARTITION = "first";
    public static final String PARTITIONS = "partitions";
    public static final String LAST_PARTITION = "last";
    public static final String NEW_COUNT_RESULT = "result";

    public static final String RESULT_COUNTER = "nResults";
}
