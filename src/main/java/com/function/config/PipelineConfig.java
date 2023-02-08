package com.function.config;



public class PipelineConfig {
    
    // ------------------------ JSON Properties used for jobs descriptions. ---------------------------
    // Name of the container where the files are uploaded
    public final static String FILE_LIST_CONTAINER_NAME = "filelist";
    
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


    
    
    public static final String AGGREGATION_ID = "aggregationID";
    public static final String TYPE_OF_CONTENT = "type_of_content";

    // ------------------------------------------------------------------------------------------------

    // -------------------------------- Number of total partitions ------------------------------------
    public static final int N_PARTITIONS = 1;

}
