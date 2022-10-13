package com.emse.worker;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.SQSEvent;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.S3Object;
import org.javatuples.Triplet;


import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.HashMap;

public class SqsEventHandler implements RequestHandler<SQSEvent, Object> {
    public String handleRequest(SQSEvent request, Context context) {
        context.getLogger().log("SQS event handler invoked");
        String timeStamp;

        for(SQSEvent.SQSMessage msg : request.getRecords()){
            timeStamp = new SimpleDateFormat("yyyy-MM-dd_HH:mm:ss").format(Calendar.getInstance().getTime());
            context.getLogger().log("Invocation started: " + timeStamp);

            String[] args = msg.getBody().split(";");
            AmazonS3 s3Client = AmazonS3ClientBuilder.defaultClient();
            S3Object s3Object = s3Client.getObject(args[0], args[1]);
            String fileName = s3Object.getKey();
            InputStreamReader streamReader = new InputStreamReader(s3Object.getObjectContent(), StandardCharsets.UTF_8);
            BufferedReader reader = new BufferedReader(streamReader);
            HashMap<String, Triplet<Integer, Double, Double>> nonProcessedData = Processing.read(reader);
            String csv = Processing.computeAndWrite(nonProcessedData, Processing.getFileData(fileName).getValue1());
            context.getLogger().log("File " + fileName + " has been processed");

            s3Client.putObject(args[0], Processing.getFileData(fileName).getValue0(), csv);
            context.getLogger().log("Processed file saved to S3");

            s3Client.deleteObject(args[0], args[1]);
            context.getLogger().log("Original file deleted from S3");

            timeStamp = new SimpleDateFormat("yyyy-MM-dd_HH:mm:ss").format(Calendar.getInstance().getTime());
            context.getLogger().log("Invocation completed: " + timeStamp);
        }

        context.getLogger().log("File processing finished");
        return "Ok";
    }
}