package com.example;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.services.comprehend.ComprehendClient;
import software.amazon.awssdk.services.comprehend.model.DetectKeyPhrasesRequest;
import software.amazon.awssdk.services.comprehend.model.DetectSentimentRequest;
import software.amazon.awssdk.services.comprehend.model.DetectSentimentResponse;
import software.amazon.awssdk.services.comprehend.model.KeyPhrase;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.sns.SnsClient;
import software.amazon.awssdk.services.sns.model.PublishRequest;
import software.amazon.awssdk.services.translate.TranslateClient;
import software.amazon.awssdk.services.translate.model.TranslateTextRequest;
import software.amazon.awssdk.services.translate.model.UnsupportedLanguagePairException;

import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;

public class FeedbackAnalyzerHandler implements RequestHandler<Map<String, Object>, Map<String, Object>> {
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final int MAX_LINES = 25;
    private static final int MAX_CHARS = 1500;
    private static final String TABLE_NAME = "FeedbackTickets";
    private static final String TOPIC_ARN = System.getenv("SNS_TOPIC_ARN");

    private static final TranslateClient TRANSLATE = TranslateClient.create();
    private static final ComprehendClient COMPREHEND = ComprehendClient.create();
    private static final SnsClient SNS = SnsClient.create();
    private static final S3Client S3 = S3Client.create();
    private static final DynamoDbClient DYNAMODB = DynamoDbClient.create();

    private static final Map<String, List<String>> CATEGORIES = Map.of(
        "refund", List.of("refund", "money back", "charged", "payment", "paid"),
        "damaged", List.of("broken", "damaged", "defective", "faulty", "cracked"),
        "delivery", List.of("delivery", "deliver", "shipping", "shipment", "arrive", "arrived", "tracking", "order", "late", "delayed"),
        "technical", List.of("app", "website", "login", "log in", "password", "error", "crash", "bug", "not working")
    );

    private static final Map<String, String> REPLIES = Map.of(
        "refund", "We are sorry about the problem with your payment. Your refund request has been passed to our billing team, who will contact you within 24 hours.",
        "damaged", "We are very sorry that your item arrived damaged. Please keep the item and its packaging, and our team will arrange a replacement or refund shortly.",
        "delivery", "We apologize for the delay with your order. Our delivery team is checking its status now, and we will update you as soon as we have news.",
        "technical", "Thank you for reporting this problem. Our technical team is looking into it and will get back to you soon.",
        "other", "Thank you for contacting us. A member of our support team will review your message and reply soon."
    );
    private static final String POSITIVE_REPLY =
        "Thank you for your kind words. We are glad you are happy with us, and we look forward to serving you again.";

    @Override
    public Map<String, Object> handleRequest(Map<String, Object> event, Context context) {
        Object recordsObj = event.get("Records");
        if (recordsObj instanceof List<?> records && !records.isEmpty()
                && records.get(0) instanceof Map<?, ?> first
                && "aws:s3".equals(first.get("eventSource"))) {
            List<Map<String, Object>> summary = new ArrayList<>();
            for (Object obj : records) {
                if (!(obj instanceof Map<?, ?> rec)) continue;
                Map<?, ?> s3Info = (Map<?, ?>) rec.get("s3");
                Map<?, ?> bucketInfo = (Map<?, ?>) s3Info.get("bucket");
                Map<?, ?> objectInfo = (Map<?, ?>) s3Info.get("object");
                String bucket = String.valueOf(bucketInfo.get("name"));
                String key = URLDecoder.decode(String.valueOf(objectInfo.get("key")), StandardCharsets.UTF_8);
                ResponseBytes<?> bytes = S3.getObjectAsBytes(GetObjectRequest.builder().bucket(bucket).key(key).build());
                String content = new String(bytes.asByteArray(), StandardCharsets.UTF_8);
                if (!content.isEmpty() && content.charAt(0) == '\uFEFF') content = content.substring(1);
                String[] lines = content.split("\\R");
                int done = 0, failed = 0, considered = 0;
                for (String raw : lines) {
                    String line = raw.trim();
                    if (line.isEmpty()) continue;
                    if (considered >= MAX_LINES) break;
                    considered++;
                    try {
                        processMessage(line, "s3:" + key);
                        done++;
                    } catch (Exception e) {
                        failed++;
                        System.err.println("Failed line: " + line.substring(0, Math.min(60, line.length())) + " : " + e);
                    }
                }
                Map<String, Object> one = new LinkedHashMap<>();
                one.put("file", key);
                one.put("processed", done);
                one.put("failed", failed);
                summary.add(one);
            }
            System.out.println("Batch summary: " + writeJson(summary));
            Map<String, Object> response = new LinkedHashMap<>();
            response.put("statusCode", 200);
            response.put("body", writeJson(summary));
            return response;
        }

        String message = event.get("message") instanceof String s ? s : null;
        if (message == null && event.get("body") instanceof String body) {
            try {
                Map<String, Object> parsed = JSON.readValue(body, new TypeReference<>() {});
                message = parsed.get("message") instanceof String s ? s : "";
            } catch (Exception e) {
                message = "";
            }
        }
        if (message == null || message.trim().isEmpty()) {
            return response(400, Map.of("error", "message is required"));
        }

        // Direct-path failures are allowed to escape so Lambda records an invocation error.
        Map<String, Object> ticket = processMessage(message, "direct");
        return response(200, ticket);
    }

    private static Map<String, Object> processMessage(String input, String source) {
        String message = input.length() > MAX_CHARS ? input.substring(0, MAX_CHARS) : input;
        String english;
        String language;

        try {
            var result = TRANSLATE.translateText(TranslateTextRequest.builder()
                .text(message).sourceLanguageCode("auto").targetLanguageCode("en").build());
            english = result.translatedText();
            language = result.sourceLanguageCode();
        } catch (UnsupportedLanguagePairException e) {
            english = message;
            language = "en";
        }

        DetectSentimentResponse sentimentResponse = COMPREHEND.detectSentiment(
            DetectSentimentRequest.builder().text(english).languageCode("en").build());
        String sentiment = sentimentResponse.sentimentAsString();

        List<KeyPhrase> phrases = COMPREHEND.detectKeyPhrases(
            DetectKeyPhrasesRequest.builder().text(english).languageCode("en").build()).keyPhrases();

        String priority = ("NEGATIVE".equals(sentiment) || "MIXED".equals(sentiment)) ? "HIGH" : "NORMAL";
        String category = classify(english);
        String replyEnglish = "POSITIVE".equals(sentiment) ? POSITIVE_REPLY : REPLIES.get(category);
        String reply = replyEnglish;

        if (!"en".equalsIgnoreCase(language)) {
            try {
                reply = TRANSLATE.translateText(TranslateTextRequest.builder()
                    .text(replyEnglish).sourceLanguageCode("en").targetLanguageCode(language).build()).translatedText();
            } catch (Exception e) {
                System.err.println("Reply translation failed: " + e);
            }
        }

        String ticketId = UUID.randomUUID().toString();
        String createdAt = Instant.now().toString();
        List<AttributeValue> phraseValues = phrases.stream()
            .map(p -> AttributeValue.builder().s(p.text()).build()).toList();

        Map<String, AttributeValue> item = new LinkedHashMap<>();
        item.put("ticket_id", AttributeValue.builder().s(ticketId).build());
        item.put("created_at", AttributeValue.builder().s(createdAt).build());
        item.put("source", AttributeValue.builder().s(source).build());
        item.put("original_text", AttributeValue.builder().s(message).build());
        item.put("language", AttributeValue.builder().s(language).build());
        item.put("english_text", AttributeValue.builder().s(english).build());
        item.put("sentiment", AttributeValue.builder().s(sentiment).build());
        item.put("key_phrases", AttributeValue.builder().l(phraseValues).build());
        item.put("category", AttributeValue.builder().s(category).build());
        item.put("priority", AttributeValue.builder().s(priority).build());
        item.put("reply_english", AttributeValue.builder().s(replyEnglish).build());
        item.put("reply", AttributeValue.builder().s(reply).build());

        DYNAMODB.putItem(PutItemRequest.builder().tableName(TABLE_NAME).item(item).build());

        if ("HIGH".equals(priority) && TOPIC_ARN != null && !TOPIC_ARN.isBlank()) {
            try {
                String email = "Ticket: " + ticketId + "\nSource: " + source + "\nCategory: " + category
                    + "\nLanguage: " + language + "\nSentiment: " + sentiment + "\nMessage (English): "
                    + english + "\nOriginal: " + message + "\nAuto-reply: " + replyEnglish;
                SNS.publish(PublishRequest.builder().topicArn(TOPIC_ARN)
                    .subject("HIGH priority ticket: " + category).message(email).build());
            } catch (Exception e) {
                System.err.println("SNS publish failed: " + e);
            }
        }

        Map<String, Object> ticket = new LinkedHashMap<>();
        ticket.put("ticket_id", ticketId);
        ticket.put("created_at", createdAt);
        ticket.put("source", source);
        ticket.put("original_text", message);
        ticket.put("language", language);
        ticket.put("english_text", english);
        ticket.put("sentiment", sentiment);
        ticket.put("key_phrases", phrases.stream().map(KeyPhrase::text).toList());
        ticket.put("category", category);
        ticket.put("priority", priority);
        ticket.put("reply_english", replyEnglish);
        ticket.put("reply", reply);
        return ticket;
    }

    private static String classify(String text) {
        String lowered = text.toLowerCase(Locale.ROOT);
        for (Map.Entry<String, List<String>> entry : CATEGORIES.entrySet()) {
            for (String word : entry.getValue()) {
                if (lowered.contains(word)) return entry.getKey();
            }
        }
        return "other";
    }

    private static Map<String, Object> response(int statusCode, Object body) {
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("statusCode", statusCode);
        result.put("body", writeJson(body));
        return result;
    }

    private static String writeJson(Object value) {
        try {
            return JSON.writeValueAsString(value);
        } catch (Exception e) {
            throw new RuntimeException("Could not serialize JSON response", e);
        }
    }
}
