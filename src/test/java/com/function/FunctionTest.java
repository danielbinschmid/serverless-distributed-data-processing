package com.function;


import com.microsoft.azure.functions.*;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.util.*;
import java.util.logging.Logger;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;



/**
 * Unit test for Function class.
 */
public class FunctionTest {
    /**
     * Unit test for HttpTriggerJava method.
     */
    @Test
    public void testHttpTriggerJava() throws Exception {
        // Setup
        @SuppressWarnings("unchecked")
        final HttpRequestMessage<Optional<String>> req = mock(HttpRequestMessage.class);

        final Map<String, String> queryParams = new HashMap<>();
        queryParams.put("name", "Azure");
        doReturn(queryParams).when(req).getQueryParameters();

        final Optional<String> queryBody = Optional.empty();
        doReturn(queryBody).when(req).getBody();

        doAnswer(new Answer<HttpResponseMessage.Builder>() {
            @Override
            public HttpResponseMessage.Builder answer(InvocationOnMock invocation) {
                HttpStatus status = (HttpStatus) invocation.getArguments()[0];
                return new HttpResponseMessageMock.HttpResponseMessageBuilderMock().status(status);
            }
        }).when(req).createResponseBuilder(any(HttpStatus.class));

        final ExecutionContext context = mock(ExecutionContext.class);
        doReturn(Logger.getGlobal()).when(context).getLogger();

        // Invoke
        // @SuppressWarnings("unchecked")
        // final OutputBinding<String> msg = (OutputBinding<String>)mock(OutputBinding.class);
        final HttpResponseMessage ret = new Function().run(req, context);

        // Verify
        assertEquals(ret.getStatus(), HttpStatus.OK);

        
        
        // String leaseID = leaseClient.acquireLease(10);
        // System.out.println("obtained lease with id: " + leaseID);
        // SharedAccessSignature=sv=2021-10-04&ss=btqf&srt=sco&st=2023-01-29T22%3A20%3A01Z&se=2023-02-10T22%3A20%3A00Z&sp=rwdxftlacup&sig=aikwt6G1xIRGPaw8h6K%2F9v5qFg0qTuI5xm37K0A2IUs%3D;BlobEndpoint=https://testhttpscript.blob.core.windows.net/;FileEndpoint=https://testhttpscript.file.core.windows.net/;QueueEndpoint=https://testhttpscript.queue.core.windows.net/;TableEndpoint=https://testhttpscript.table.core.windows.net/;
        // String h = "here";
        // BinaryData data = BinaryData.fromBytes(h.getBytes());
        // blobClient.upload(data);;

        // DefaultAzureCredential defaultCredential = new DefaultAzureCredentialBuilder().build();
        //     
        // // Azure SDK client builders accept the credential as a parameter
        //BlobServiceClient blobServiceClient = new BlobServiceClientBuilder()
        //        .endpoint("https://testhttpscript.blob.core.windows.net/")
        //         .credential(defaultCredential)
        //        .buildClient(); // https://testhttpscript.blob.core.windows.net/tasks/yaya.txt
        // BlobContainerClient client = blobServiceClient.getBlobContainerClient("tasks"q);
        //         
        // BlobClient blobClient = client.getBlobClient("a");
        //     
        // BlobLeaseClient leaseClient = new BlobLeaseClientBuilder()
        //                 .blobClient(blobClient)
        //                 .buildClient();
        //         
        // String leaseID = leaseClient.acquireLease(10);
        // System.out.println("obtained lease with id: " + leaseID);
    }
}
