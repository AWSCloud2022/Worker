package com.emse.worker;

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

    public static String[] readFirstMessage(List<Message> messages) {
        String content = messages.get(0).body();
        return content.split(";");
    }

    public static void deleteFirstMessage(SqsClient sqs, String queueURL, List<Message> messages) {
        try {
            DeleteMessageRequest deleteMessageRequest = DeleteMessageRequest.builder()
                    .queueUrl(queueURL)
                    .receiptHandle(messages.get(0).receiptHandle())
                    .build();
            sqs.deleteMessage(deleteMessageRequest);
            System.out.println("Message successfully deleted from SQS");
        } catch (SqsException e) {
            throw new RuntimeException(e);
        }
    }
}
