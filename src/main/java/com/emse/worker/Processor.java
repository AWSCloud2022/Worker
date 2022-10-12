package com.emse.worker;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.S3Object;
import org.javatuples.Triplet;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Scanner;

public class Processor {
    public static void main(String[] args) { //to modify to handle all requests queued in SQS...
        Scanner scan = new Scanner(System.in);
        System.out.println("Type bucket name and file name:");
        String[] input = scan.nextLine().split("\\s+");

        AmazonS3 s3Client = AmazonS3ClientBuilder.defaultClient();
        S3Object s3Object = s3Client.getObject(input[0], input[1]);
        String fileName = s3Object.getKey();
        InputStreamReader streamReader = new InputStreamReader(s3Object.getObjectContent(), StandardCharsets.UTF_8);
        BufferedReader reader = new BufferedReader(streamReader);
        HashMap<String, Triplet<Integer, Double, Double>> nonProcessedData = Processing.read(reader);
        String csv = Processing.computeAndWrite(nonProcessedData, Processing.getFileData(fileName).getValue1());
        s3Client.putObject(input[0], Processing.getFileData(fileName).getValue0(), csv);
        s3Client.deleteObject(input[0], input[1]);

        System.out.println("File processing finished");
    }
    //test with: databucket8906 01-10-2022-store2.csv
}
