package com.function;


import com.microsoft.azure.functions.ExecutionContext;
import com.microsoft.azure.functions.HttpMethod;
import com.microsoft.azure.functions.HttpRequestMessage;
import com.microsoft.azure.functions.HttpResponseMessage;
import com.microsoft.azure.functions.HttpStatus;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.blob.specialized.BlobLeaseClient;
import com.azure.storage.blob.specialized.BlobLeaseClientBuilder;
import com.microsoft.azure.functions.annotation.AuthorizationLevel;
import com.microsoft.azure.functions.annotation.FunctionName;
import com.microsoft.azure.functions.annotation.HttpTrigger;
import com.function.config.AccountConfig;
import java.util.Optional;


/**
 * Azure Functions with HTTP Trigger.
 */
public class Function {

    /**
     * This function listens at endpoint "/api/HttpExample". Two ways to invoke it using "curl" command in bash:
     * 1. curl -d "HTTP Body" {your hostq}/api/HttpExample
     * 2. curl "{your host}/api/HttpExample?name=HTTP%20Query"
     */
    @FunctionName("HttpExample")
    public HttpResponseMessage run(
            @HttpTrigger(
                name = "req",
                methods = {HttpMethod.GET, HttpMethod.POST},
                authLevel = AuthorizationLevel.ANONYMOUS)
                HttpRequestMessage<Optional<String>> request,
            final ExecutionContext context) {
        context.getLogger().info("Java HTTP trigger processed a request.");

           
        // DefaultAzureCredential defaultCredential = new DefaultAzureCredentialBuilder().build();
        BlobServiceClient blobServiceClient = new BlobServiceClientBuilder()
            .endpoint(AccountConfig.BLOB_STORAGE_ACC_ENDPOINT)
            .sasToken(AccountConfig.BLOB_STORAGE_ACC_SAS_TOKEN)
            .buildClient();
        
        BlobContainerClient client = blobServiceClient.getBlobContainerClient("tasks");
        
        BlobClient blobClient = client.getBlobClient("a");
        // String h = "here";
        // BinaryData data = BinaryData.fromBytes(h.getBytes());
        // blobClient.upload(data);;
        BlobLeaseClient leaseClient = new BlobLeaseClientBuilder()
                        .blobClient(blobClient)
                        .buildClient();

        System.out.println("a " + leaseClient.getLeaseId());
        
        String leaseID = "none";
        try {
            leaseID = leaseClient.acquireLease(20);
        } catch (Exception e) {
            leaseID = "blob can not be leased"; 
        }  
        
        System.out.println("obtained lease with id: " + leaseID);

        return request.createResponseBuilder(HttpStatus.OK).body("obtained lease with id: " + leaseID).build();

    }
}
