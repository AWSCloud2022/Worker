package com.emse.worker;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.S3Object;
import com.emse.worker.helper.Processing;
import com.emse.worker.helper.SqsManager;
import com.jayway.jsonpath.JsonPath;
import org.javatuples.Triplet;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.Message;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Timer;
import java.util.TimerTask;

public class Processor {
    //Enter the url of the SQS queue you wish to be accessed by the Worker
    private static final String sqsUrl = "https://sqs.us-east-1.amazonaws.com/818564790073/demoQueueApp";

    public static void main(String[] args) {
        long interval = 120L;

        if (args.length != 0) {
            if (args.length == 2 && args[0].equals("--interval")) {
                try {
                    interval = Integer.parseInt(args[1]);
                    System.out.println(ANSI_BLUE + "Interval set to " + args[1] + " seconds" + ANSI_WHITE);
                } catch (NumberFormatException nfe) {
                    System.out.println(ANSI_RED + "Interval value " + args[1] + " seconds is invalid; defaulted to 120 seconds" + ANSI_WHITE);
                }
            } else {
                System.out.println(ANSI_RED + "The arguments you entered are invalid");
                System.out.println("Only argument accepted is '--delay <delay in seconds>'");
                System.out.println("Interval defaulted to 120 seconds" + ANSI_WHITE);
            }
        }

        System.out.println(ANSI_GREEN+ "The Worker is now active" + ANSI_WHITE);
        System.out.println(ANSI_GREEN+ "First processing in 10 seconds" + ANSI_WHITE);

        Timer timer = new Timer();
        final long interval2 = interval;
        TimerTask task = new TimerTask() {
            @Override
            public void run() {
                process();
                System.out.println("Next check in " + interval2 + " seconds");
            }
        };

        timer.schedule(task, 10000L, interval * 1000L);
    }

    private static void process() {
        //long startTime = System.currentTimeMillis();

        SqsClient sqsClient = SqsClient.builder().httpClientBuilder(UrlConnectionHttpClient.builder()).build();

        for(Message msg : SqsManager.receiveMessages(sqsClient, sqsUrl)) {
            String json = msg.body();
            String message = JsonPath.parse(json).read("$.Message");
            System.out.println(message);
            String[] info = message.split(";");
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

            SqsManager.deleteMessage(sqsClient, sqsUrl, msg); //explicitly delete the message in the SQS queue
            System.out.println("Deleted SQS message");
        }
        //long elapsedTime = System.currentTimeMillis() - startTime;
        //System.out.println(ANSI_BLUE + "Total elapsed time: " + elapsedTime*10e-3 + " seconds" + ANSI_WHITE);
    }

    private static final String ANSI_GREEN = "\u001b[32m";
    private static final String ANSI_RED = "\u001b[31m";
    private static final String ANSI_WHITE = "\u001b[37m";
    private static final String ANSI_BLUE = "\u001b[34m";
}
