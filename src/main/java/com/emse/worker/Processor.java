package com.emse.worker;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.S3Object;
import org.javatuples.Triplet;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.Message;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.HashMap;

public class Processor {
    public static void main(String[] args) { //to modify to handle all requests queued in SQS...
        SqsClient sqsClient = SqsClient.builder().httpClient(UrlConnectionHttpClient.builder().build()).build();;
        String url = "https://sqs.us-east-1.amazonaws.com/818564790073/StoreSalesUploadQueue";

        System.out.println("SQS event handler invoked");
        String timeStamp;

        for(Message msg : SqsManager.receiveMessages(sqsClient, url)){
            timeStamp = new SimpleDateFormat("yyyy-MM-dd_HH:mm:ss").format(Calendar.getInstance().getTime());
            System.out.println("Invocation started: " + timeStamp);

            String[] info = msg.body().split(";");
            AmazonS3 s3Client = AmazonS3ClientBuilder.defaultClient();
            S3Object s3Object = s3Client.getObject(info[0], info[1]);
            String fileName = s3Object.getKey();
            InputStreamReader streamReader = new InputStreamReader(s3Object.getObjectContent(), StandardCharsets.UTF_8);
            BufferedReader reader = new BufferedReader(streamReader);
            HashMap<String, Triplet<Integer, Double, Double>> nonProcessedData = Processing.read(reader);
            String csv = Processing.computeAndWrite(nonProcessedData, Processing.getFileData(fileName).getValue1());
            System.out.println("File " + fileName + " has been processed");

            s3Client.putObject(info[0], Processing.getFileData(fileName).getValue0(), csv);
            System.out.println("Processed file saved to S3");

            s3Client.deleteObject(info[0], info[1]);
            System.out.println("Original file deleted from S3");

            SqsManager.deleteMessage(sqsClient, url, msg);
            System.out.println("Deleted SQS message");

            timeStamp = new SimpleDateFormat("yyyy-MM-dd_HH:mm:ss").format(Calendar.getInstance().getTime());
            System.out.println("Invocation completed: " + timeStamp);
        }

        System.out.println("File processing finished");
    }
    //test with: databucket8906 01-10-2022-store2.csv
}
