package com.function.config;

public class AccountConfig {
    
    public static final String STORAGE_ACC_NAME = "cbdp261a573";
    public static final String STORAGE_ACC_PRIM_KEY = "1Ac1BhMZGibSt+lPxXGdEQKM5sID74KMb1M9G/6DuVS4pdNVwmVKdYXYa7Kd211LRlCSIDMa6EDi+AStmOZdoQ==";

    // SAS token for blob-access of storage account.
    public static final String STORAGE_ACC_SAS_TOKEN = "?sv=2021-10-04&ss=btqf&srt=sco&st=2023-02-06T17%3A01%3A50Z&se=2023-03-10T17%3A01%3A00Z&sp=rwdxftlup&sig=7BJkC2lIHnncsWFjTCsltFmR8TGP%2BRJVbQMD8jYTK6M%3D";
    
    // ----------------------- Storage account access configuration strings. --------------------------

    // url endpoint to access storage account for blobs.
    public static final String BLOB_STORAGE_ACC_ENDPOINT = "https://" + STORAGE_ACC_NAME + ".blob.core.windows.net/";
    
    // Connection string for connecting with Azure storage queues.
    public static final String CONNECTION_STRING = "DefaultEndpointsProtocol=https;AccountName=" + STORAGE_ACC_NAME + ";AccountKey=" + STORAGE_ACC_PRIM_KEY + ";EndpointSuffix=core.windows.net";

    // ------------------------------------------------------------------------------------------------
    // SharedAccessSignature=sv=2021-10-04&ss=btqf&srt=sco&st=2023-02-06T17%3A01%3A50Z&se=2023-03-10T17%3A01%3A00Z&sp=rwdxftlup&sig=7BJkC2lIHnncsWFjTCsltFmR8TGP%2BRJVbQMD8jYTK6M%3D;BlobEndpoint=https://cbdp261a573.blob.core.windows.net/;FileEndpoint=https://cbdp261a573.file.core.windows.net/;QueueEndpoint=https://cbdp261a573.queue.core.windows.net/;TableEndpoint=https://cbdp261a573.table.core.windows.net/;
    // ?sv=2021-10-04&ss=btqf&srt=sco&st=2023-02-06T17%3A01%3A50Z&se=2023-03-10T17%3A01%3A00Z&sp=rwdxftlup&sig=7BJkC2lIHnncsWFjTCsltFmR8TGP%2BRJVbQMD8jYTK6M%3D    
    // public static final String CONNECTION_STRING = "DefaultEndpointsProtocol=https;AccountName=<your_name_of_the_function_app>;AccountKey=<your_account_key>;EndpointSuffix=core.windows.net";
    // 
}


