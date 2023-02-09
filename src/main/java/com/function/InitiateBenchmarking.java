package com.function;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLConnection;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;

import org.json.simple.parser.JSONParser;

import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.function.config.AccountConfig;
import com.function.config.BlobPipelineConfig;
import com.function.config.QueuePipelineConfig;
import com.function.pipelines.helper.BlobContainerWrapper;

import org.json.simple.JSONObject;

class RunnableDemo implements Runnable {
    private Thread t;
    private String threadName;
    private static final String USER_AGENT = "Mozilla/5.0";
    private String filename;
    private String postURL;
    private String postParams;
    RunnableDemo( String name, String filename, String postURL, String postParams) {
        threadName = name;
        this.postURL = postURL;
        this.filename = filename;
        this.postParams = postParams;
        System.out.println("Creating " +  threadName );
    }

    


    private void sendPOST(String postParams) throws IOException {
		URL obj = new URL(postURL);
		HttpURLConnection con = (HttpURLConnection) obj.openConnection();
		con.setRequestMethod("POST");
		con.setRequestProperty("User-Agent", USER_AGENT);

		// For POST only - START
		con.setDoOutput(true);
		OutputStream os = con.getOutputStream();
		os.write(postParams.getBytes());
		os.flush();
		os.close();
		// For POST only - END

		int responseCode = con.getResponseCode();
		System.out.println("POST Response Code :: " + responseCode);

		if (responseCode == HttpURLConnection.HTTP_OK) { //success
			BufferedReader in = new BufferedReader(new InputStreamReader(con.getInputStream()));
			String inputLine;
			StringBuffer response = new StringBuffer();

			while ((inputLine = in.readLine()) != null) {
				response.append(inputLine);
			}
			in.close();

			// print result
			System.out.println(response.toString());
		} else {
			System.out.println("POST request did not work.");
		}
	}

    public void run() {
       try {
            
            sendPOST(postParams);
            System.out.println("Sent POST for " + filename);
       } catch (Exception e) {
          System.out.println("Thread " +  threadName + " responsible of sending POST of filename: " + filename + " interrupted.");
       }
       
    }
    
    public void start () {
       System.out.println("Starting " +  threadName );
       if (t == null) {
          t = new Thread (this, threadName);
          t.start ();
       }
    }
 }


public class InitiateBenchmarking {


    /**
     * 
     * @param args Usage: <folder path to benchmark files> <post_url> <queue_type> <benchmark_run_id>
     * @throws Exception
     */
	public static void main(String[] args) throws Exception {

        if (args.length != 4) System.out.println("Usage: <folder path to benchmark files> <post_url_api (without queuetype suffix)> <queue_type> <benchmark_run_id>"); 
        
        String folderPath = args[0];
        String queueType = args[2];
        String postURL = args[1] + queueType;
        
        String benchmarkID = args[3];
        System.out.println("Benchmarking for batches in folder path: " + folderPath + "; for post url: " + postURL + "; with benchmark id: " + benchmarkID);

        try {
           
            File folder = new File(folderPath);

            File[] listOfFiles = folder.listFiles();

            ArrayList<RunnableDemo> threads = new ArrayList<>();

            for (File file: listOfFiles) {
                String filename = file.getName();
                System.out.println(filename);
                JSONParser parser = new JSONParser();
                JSONObject a = (JSONObject) parser.parse(new FileReader(folderPath + filename));
                String postParams = a.toString();

                RunnableDemo thread = new RunnableDemo( "Thread-" + filename, filename, postURL, postParams);
                threads.add(thread);
            }

            // upload file 
            uploadReferenceBlob(benchmarkID, queueType);
            System.out.println("REFERENCE BLOB UPLOADED");

            for (RunnableDemo thread: threads) {//  
                thread.start(); 
            }
        } catch(Exception e) { System.err.println(e.getMessage()); }
	}


    private static void uploadReferenceBlob(String blobName, String queueType) {
        if (queueType.compareTo("queue") == 0) {
            BlobContainerWrapper resultBlobContainerWrapper = new BlobContainerWrapper(QueuePipelineConfig.RESULTS_BLOB_CONTAINER);
            resultBlobContainerWrapper.writeFile(blobName, "pipeline triggered.");
        } else if (queueType.compareTo("blob") == 0) {
            BlobContainerWrapper resultBlobContainerWrapper = new BlobContainerWrapper(BlobPipelineConfig.MERGE_RESULT_CONTAINER_NAME);
            resultBlobContainerWrapper.writeFile(blobName, "pipeline triggered.");
        } else {
            System.out.println("queue type not known " + queueType);
        }
        
    }

    


	
}
