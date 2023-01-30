package com.function;

import com.microsoft.azure.functions.ExecutionContext;
import com.microsoft.azure.functions.annotation.*;
import org.json.JSONObject;

public class BlobUploadTrigger {

    @FunctionName("BlobUploadTrigger")
    @StorageAccount("BlobConnectionString")
    public void run(
            @BlobTrigger(name = "file",
                    dataType = "binary",
                    path = "filelists/{name}.{extension}",
                    connection = "AzureWebJobsStorage") byte[] content,
            @BindingName("name") String filename,
            final ExecutionContext context
    ) {
        // Count all lines
        long n = 0;
        for (byte b : content) {
            if (b == '\n') {
                n++;
            }
        }

        BlobContainerWrapper blobContainerWrapper = new BlobContainerWrapper("aggregationjobs");
        // Create ranges for aggregates
        // Try out with different number of range lengths
        long rangeLength = n / 10;

        for (int i = 0; i < 10; i++) {
            long rangeStart = i * rangeLength;
            long rangeEnd = (i + 1) * rangeLength;

            if (rangeEnd > n) {
                rangeEnd = n;
            }
            JSONObject jsonObject = new JSONObject();

            jsonObject.put("target", filename + ".csv");
            jsonObject.put("rangeStart", rangeStart);
            jsonObject.put("rangeEnd", rangeEnd);
            jsonObject.put("container", "filelists");

            blobContainerWrapper.writeFile(filename + "." + i + ".json", jsonObject.toString());
        }

        context.getLogger().info("Processed file: " + filename);

    }

}
