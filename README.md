Multilingual Feedback Analyzer using AWS

🏗️ Architecture

Amazon S3 / Lambda Test Event → AWS Lambda → Amazon Translate → Amazon Comprehend → Business Rules → Amazon DynamoDB / Amazon SNS

AWS Lambda coordinates the workflow, while IAM manages permissions and CloudWatch provides logging and monitoring.

<img width="648" height="284" alt="image" src="https://github.com/user-attachments/assets/6fb1a658-33fd-4a79-89af-44e215d6bc35" />

📌 Project Overview

The Multilingual Feedback Analyzer is a serverless AWS project that processes customer feedback written in different languages. It uses AWS AI services to translate feedback, analyze sentiment, identify key phrases, categorize customer issues, and generate appropriate replies.

The system stores feedback analysis results in Amazon DynamoDB and sends email notifications through Amazon SNS when high-priority negative feedback is detected.

🎯 Objectives

Process customer feedback in multiple languages.

Translate feedback into English for analysis.

Detect customer sentiment and important key phrases.

Categorize feedback into common support issues.

Generate automated replies in the customer's original language.

Store feedback tickets in a database.

Send email alerts for high-priority feedback.

<img width="494" height="269" alt="image" src="https://github.com/user-attachments/assets/e499e95d-9e40-47b1-9a4e-2c0b939f512f" />

⚙️ How It Works

Input: Customer feedback is submitted directly through a Lambda test event or uploaded as a text file to Amazon S3.

Translation: Amazon Translate converts feedback into English for consistent analysis.

Sentiment Analysis: Amazon Comprehend identifies whether the feedback is positive, negative, or mixed.

Key Phrase Extraction: Important words and phrases are extracted from the feedback.

Issue Categorization: Python business rules categorize feedback as Refund, Damaged Product, Delivery, Technical Issue, or Other.

Priority Assignment: Negative or mixed sentiment is marked as high priority.

Automated Reply: A suitable response is generated and translated into the customer's original language when supported.

Data Storage: Feedback details, sentiment, category, priority, and reply are saved in Amazon DynamoDB.

Email Notification: Amazon SNS sends an alert for high-priority feedback when configured.

Monitoring: Amazon CloudWatch logs Lambda execution details and can trigger email alerts through CloudWatch alarms.





