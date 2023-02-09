package com.function.config;

public class AccountConfig {

    public static final String STORAGE_ACC_NAME = "cbdpqueuec7faf4";
    public static final String STORAGE_ACC_PRIM_KEY = "SlzkntSPPlxIoIV3rhYcfVpnJl3QSO7ja6P5M7FjUQZfXVmQXNxuIxhsp0owzceSAi2szjfQpgwB+AStKfLXUA==";

    // SAS token for blob-access of storage account.
    public static final String STORAGE_ACC_SAS_TOKEN = "?sv=2021-10-04&ss=btqf&srt=sco&st=2023-02-09T01%3A49%3A18Z&se=2023-02-10T01%3A49%3A18Z&sp=rwdxftlacup&sig=KOaqiFNesvtlwrsXqUfunv5c1zUtaKhGNSXdpUXp%2FUI%3D";
    
    // ----------------------- Storage account access configuration strings. --------------------------

    // url endpoint to access storage account for blobs.
    public static final String BLOB_STORAGE_ACC_ENDPOINT = "https://" + STORAGE_ACC_NAME + ".blob.core.windows.net/";
    
    // Connection string for connecting with Azure storage queues.
    public static final String CONNECTION_STRING = "DefaultEndpointsProtocol=https;AccountName=" + STORAGE_ACC_NAME + ";AccountKey=" + STORAGE_ACC_PRIM_KEY + ";EndpointSuffix=core.windows.net";

    // ------------------------------------------------------------------------------------------------

}

