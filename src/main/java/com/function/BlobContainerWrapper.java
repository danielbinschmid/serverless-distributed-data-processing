package com.function;

import com.azure.core.http.rest.PagedIterable;
import com.azure.core.util.BinaryData;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobContainerClientBuilder;
import com.azure.storage.blob.models.BlobItem;
import com.function.config.AccountConfig;

import java.util.concurrent.atomic.AtomicInteger;

public class BlobContainerWrapper
{
    private final BlobContainerClient blobContainerClient;

    public BlobContainerWrapper(String containerName) {
        this.blobContainerClient = new BlobContainerClientBuilder()
                .endpoint(AccountConfig.BLOB_STORAGE_ACC_ENDPOINT)
                .sasToken(AccountConfig.BLOB_STORAGE_ACC_SAS_TOKEN)
                .containerName(containerName)
                .buildClient();
    }

    public void writeFile(String filename, String content) {
            try {
                BlobClient blobClient = blobContainerClient.getBlobClient(filename);
                blobClient.upload(BinaryData.fromString(content), true);
            } catch (Exception e) {
                System.err.println("EXCEPTION: " + e.getMessage());
            }
    }

    public BinaryData readFile(String filename) {
        BinaryData binaryData = null;
        try {
            BlobClient blobClient = blobContainerClient.getBlobClient(filename);
            binaryData = blobClient.downloadContent();
        } catch (Exception e) {
            System.err.println("EXCEPTION: " + e.getMessage());
        }
        return binaryData;
    }

    public boolean shouldStartMerging(String filename, int count) {
        final AtomicInteger curr = new AtomicInteger();
        blobContainerClient.listBlobs().forEach(blob -> {
            if (blob.getName().contains(filename)) {
                curr.addAndGet(1);
            }
        });
        return curr.get() == count;
    }

    public PagedIterable<BlobItem> getAllBlobs() {
        return blobContainerClient.listBlobs();
    }
}
