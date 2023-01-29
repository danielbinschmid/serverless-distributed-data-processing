package com.function;

import com.microsoft.azure.functions.ExecutionContext;
import com.microsoft.azure.functions.OutputBinding;
import com.microsoft.azure.functions.annotation.*;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.List;

public class BlobUploadTrigger {

    //TODO: upload container initial files
    // aggregationJobDescription for counting task
    // aggregationState -
    // We don't care about which file the results come
    // finalResult ->

    //TODO: Configure AzureWebJobsStorage as an environment variable in InteliJ of your run configuration
    // Otherwise it fails :D
    @FunctionName("BlobUploadTrigger")
    @StorageAccount("BlobConnectionString")
    public void run(
            @BlobTrigger(name = "file",
                    dataType = "binary",
                    path = "filelists/{name}.{extension}",
                    connection = "AzureWebJobsStorage") byte[] content,
            @BlobOutput(
                    name = "target",
                    path = "aggregationjobs/{name}.json",
                    connection = "AzureWebJobsStorage")
            OutputBinding<String> outputItem,
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


        List<JSONObject> list = new ArrayList<>();
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

            jsonObject.put("target", filename);
            jsonObject.put("rangeStart", rangeStart);
            jsonObject.put("rangeEnd", rangeEnd);

            list.add(jsonObject);
        }

        for (JSONObject obj : list) {
            context.getLogger().info(obj.toString());
        }

        outputItem.setValue(list.get(0).toString());

    }

}

/*
 1. It splits the main file into N subfiles
 2. For each subfile, we have a different function that processes it.
 3. n Aggregates that look like {nationKey: acc_bal, ...}
 4. Merge them

    if (prevState) {
    merge
    }

    creatState()

    -> {
        1: 5300€
        2: 100€,
        ...
    }

 **
 */
