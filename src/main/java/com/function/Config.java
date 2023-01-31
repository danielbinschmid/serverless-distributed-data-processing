package com.function;

public class Config {
    // ----------------------- Storage account access configuration strings. --------------------------
    
    // url endpoint to access storage account for blobs.
    public static final String BLOB_STORAGE_ACC_ENDPOINT = "https://<your_storage_account_name>.blob.core.windows.net/";
    // SAS token for blob-access of storage account. 
    public static final String BLOB_STORAGE_ACC_SAS_TOKEN = "<your_storage_account_sas_token>";
    // Connection string for connecting with Azure storage queues.
    public static final String CONNECTION_STRING = "<your_storage_account_connection_string>";

    // ------------------------------------------------------------------------------------------------

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

    // Blob container which contains all file inputs
    public static final String TASKS_BLOB_CONTAINER = "tasks";
    // Blob container which contains the results of the pipeline
    public static final String RESULTS_BLOB_CONTAINER = "results";

    // ------------------------------------------------------------------------------------------------

    // filename of result state blob. 
    public static final String FINAL_RESULTS_STATE_BLOB_NAME = "results.json";

    // filename of result output blob. 
    public static final String FINAL_RESULTS_OUTPUT_BLOB_NAME = "results_avgs.json";
}


