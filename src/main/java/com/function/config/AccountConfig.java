package com.function.config;

public class AccountConfig {
    //TODO: Connection string for blob upload triggers is saved in AzureWebJobsStorage; Configure that before you run the functions

    // ----------------------- Storage account access configuration strings. --------------------------

    // url endpoint to access storage account for blobs.
    public static final String BLOB_STORAGE_ACC_ENDPOINT = "https://<your_storage_account_name>.blob.core.windows.net/";
    // SAS token for blob-access of storage account.
    public static final String BLOB_STORAGE_ACC_SAS_TOKEN = "<your_storage_account_sas_token>";
    // Connection string for connecting with Azure storage queues.
    public static final String CONNECTION_STRING = "<your_storage_account_connection_string>";

    // ------------------------------------------------------------------------------------------------

}

