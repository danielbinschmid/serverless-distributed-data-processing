package com.function.config;

public class AccountConfig {

    public static final String STORAGE_ACC_NAME = "<your_storage_acc_name>";
    public static final String STORAGE_ACC_PRIM_KEY = "<your_storage_acc_primary_key>";

    // SAS token for blob-access of storage account.
    public static final String STORAGE_ACC_SAS_TOKEN = "<your_storage_acc_sas_token>";
    
    // ----------------------- Storage account access configuration strings. --------------------------

    // url endpoint to access storage account for blobs.
    public static final String BLOB_STORAGE_ACC_ENDPOINT = "https://" + STORAGE_ACC_NAME + ".blob.core.windows.net/";
    
    // Connection string for connecting with Azure storage queues.
    public static final String CONNECTION_STRING = "DefaultEndpointsProtocol=https;AccountName=" + STORAGE_ACC_NAME + ";AccountKey=" + STORAGE_ACC_PRIM_KEY + ";EndpointSuffix=core.windows.net";

    // ------------------------------------------------------------------------------------------------

}

