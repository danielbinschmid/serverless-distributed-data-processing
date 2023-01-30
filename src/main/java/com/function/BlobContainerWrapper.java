package com.function;

import com.azure.core.util.BinaryData;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobContainerClientBuilder;

public class BlobContainerWrapper
{
   private static final String endpoint = "https://azurefunctiondpstorage.blob.core.windows.net/?sv=2021-06-08&ss=bfqt&srt=sco&sp=rwdlacupiytfx&se=2023-01-29T23:15:19Z&st=2023-01-29T15:15:19Z&spr=https&sig=YvuCCJ%2Fm6g0oaYbIkZYsMihDyBrRpBhzJbndngTVvY0%3D";
    private final BlobContainerClient blobContainerClient;

    public BlobContainerWrapper(String containerName) {
        this.blobContainerClient = new BlobContainerClientBuilder()
                .endpoint(endpoint)
                .containerName(containerName)
                .buildClient();
    }

    public void writeFile(String filename, String content) {
            try {
                BlobClient blobClient = blobContainerClient.getBlobClient(filename);
                blobClient.upload(BinaryData.fromString(content));
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

}
