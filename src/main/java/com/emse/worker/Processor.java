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
import java.util.HashMap;
import java.util.Scanner;
import java.util.Timer;
import java.util.TimerTask;

public class Processor implements Runnable{
    private static final String ANSI_GREEN = "\u001b[32m";
    private static final String ANSI_RED = "\u001b[31m";
    private static final String ANSI_WHITE = "\u001b[37m";
    private static final String ANSI_BLUE = "\u001b[34m";

    volatile boolean keepRunning;
    @Override
    public void run() {
        while (keepRunning) {
            process();
            try {
                Thread.sleep(120000);
            } catch (InterruptedException ignored) {}
        }
    }

    private static void process() {
        System.out.println("SQS message handler invoked");
        long startTime = System.currentTimeMillis();

        SqsClient sqsClient = SqsClient.builder().httpClientBuilder(UrlConnectionHttpClient.builder()).build();
        String url = "https://sqs.us-east-1.amazonaws.com/818564790073/ManualStoreSalesUploadQueue";

        for(Message msg : SqsManager.receiveMessages(sqsClient, url)){
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
        }

        System.out.println("File processing finished");

        long elapsedTime = System.currentTimeMillis() - startTime;
        System.out.println(ANSI_BLUE + "Total elapsed time: " + elapsedTime*10e-3 + " seconds" + ANSI_WHITE);
    }

    public static void main(String[] args) {
        long interval = 120;

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

        Timer timer = new Timer();
        TimerTask task = new TimerTask() {
            @Override
            public void run() {
                process();
            }
        };

        timer.schedule(task, 10000L, interval * 1000);
    }




    //Can use the following more sophisticated program if multiple cores are available

//    public static void main(String[] args) {
//        long delay = 0;
//        Processor p = new Processor();
//        Thread t = new Thread(p);
//
//        if (args.length != 0) {
//            if (args.length == 2 && args[0].equals("--delay")) {
//                try {
//                    delay = Integer.parseInt(args[1]);
//                    System.out.println(ANSI_BLUE + "Delay set to " + args[1] + " seconds" + ANSI_WHITE);
//                } catch (NumberFormatException nfe) {
//                    System.out.println(ANSI_RED + "Delay value " + args[1] + " seconds is invalid; defaulted to 0" + ANSI_WHITE);
//                }
//            }
//            else {
//                System.out.println(ANSI_RED + "The arguments you entered are invalid");
//                System.out.println("Only argument accepted is '--delay <delay in seconds>'");
//                System.out.println("Delay defaulted to 0" + ANSI_WHITE);
//            }
//        }
//
//        try {
//            Thread.sleep(delay * 1000);
//        } catch (InterruptedException ignored) {}
//
//        t.start();
//        p.run();
//
//        System.out.println(ANSI_GREEN + "Enter commands:");
//        System.out.println("Use 'help' to see command" + ANSI_WHITE);
//        Scanner s = new Scanner(System.in);
//
//        while (true) {
//            String[] input = s.nextLine().split("\\s+");
//
//            if (input.length != 1) {
//                System.out.println(ANSI_RED + "Please send one argument at a time" + ANSI_WHITE);
//            }
//            else {
//                switch (input[0]) {
//                    case "stop" : {
//                        p.keepRunning = false;
//                        t.interrupt();
//                        System.exit(0);
//                    }
//                    case "process" : process();
//                    case "pause" : {
//                        p.keepRunning = false;
//                        System.out.println(ANSI_BLUE + "Processing has been stop" + ANSI_WHITE);
//                    }
//                    case "restart" : {
//                        p.keepRunning = true;
//                        System.out.println(ANSI_BLUE + "Unpaused the process" + ANSI_WHITE);
//                    }
//                    case "help" : System.out.println(ANSI_BLUE + "stop, process, pause, restart, help" + ANSI_WHITE);
//                    default : System.out.println(ANSI_RED + "No such command" + ANSI_WHITE);
//                }
//            }
//        }
//    }

    //test with: databucket8906 01-10-2022-store2.csv
}
