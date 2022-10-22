package com.emse.worker.helper;

import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;

import java.util.List;

public class SqsManager {
    public static List<Message> receiveMessages(SqsClient sqs, String queueURL) {
        try {
            ReceiveMessageRequest receiveRequest = ReceiveMessageRequest.builder().queueUrl(queueURL).build();
            ReceiveMessageResponse sqsResponse = sqs.receiveMessage(receiveRequest);
            return sqsResponse.messages();
        } catch (SqsException e) {
            throw new RuntimeException(e);
        }
    }

    public static void deleteMessage(SqsClient sqs, String queueURL, Message msg) {
        try {
            DeleteMessageRequest deleteMessageRequest = DeleteMessageRequest.builder()
                    .queueUrl(queueURL)
                    .receiptHandle(msg.receiptHandle())
                    .build();
            sqs.deleteMessage(deleteMessageRequest);
        } catch (SqsException e) {
            throw new RuntimeException(e);
        }
    }
}
