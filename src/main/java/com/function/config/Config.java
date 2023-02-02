package com.function.config;

public class Config {
    //TODO: Connection string for blob upload triggers is saved in AzureWebJobsStorage; Configure that before you run the functions

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

    public final static String AGGREGATION_ID = "aggregationID";
    public final static String TYPE_OF_CONTENT = "type";
    public final static String NEW_COUNT_RESULT = "result";

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


    // ----------------------------- Storage account container names. ---------------------------------
    // Name of the container where the files are uploaded
    public final static String FILE_LIST_CONTAINER_NAME = "filelists";

    // Names of the containers where the aggregation jobs descriptions are uploaded
    public final static String AGGREGATION_JOBS_CONTAINER_NAME = "aggregationjobs";
    // Names of the containers where the aggregation results are uploaded
    public final static String AGGREGATION_RESULTS_CONTAINER_NAME = "aggregationresults";

    // Names of the containers where the merging jobs descriptions are uploaded
    public final static String MERGING_JOBS_CONTAINER_NAME = "mergingjobs";
    // Names of the containers where the merge result is uploaded
    public final static String MERGE_RESULT_CONTAINER_NAME = "mergeresult";

    // ------------------------------------------------------------------------------------------------

    // -------------------------------- Filenames used for writing. -----------------------------------
    public static final String MERGE_RESULT_FILENAME = "merge-result.json";
    // ------------------------------------------------------------------------------------------------

    // ------------------------ JSON Properties used for jobs descriptions. ---------------------------
    // Common
    public static final String JOB_CONTAINER_PROP = "container";
    
    // Aggregations
    public static final String AGGREGATION_JOB_TARGET = "target";
    public static final String AGGREGATION_JOB_RANGE_START = "rangeStart";
    public static final String AGGREGATION_JOB_RANGE_END = "rangeEnd";
    
    // Merging
    public static final String MERGING_JOB_PREFIX = "prefixname";
    public static final String MERGE_RESULT_SUM = "sum";
    public static final String MERGE_RESULT_COUNT = "count";
    // ------------------------------------------------------------------------------------------------

    // -------------------------------- Partitions ------------------------------------
    // TODO: Experiment with different number of partitions
    public static final int N_PARTITIONS = 10;

    public static final String FIRST_PARTITION = "firstPartition";
    public static final String LAST_PARTITION = "lastPartition";
    // ------------------------------------------------------------------------------------------------
    
}
