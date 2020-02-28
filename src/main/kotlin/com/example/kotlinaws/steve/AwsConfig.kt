package com.example.kotlinaws.steve
// With James

import com.amazonaws.regions.Regions
import com.amazonaws.services.s3.AmazonS3ClientBuilder
import org.springframework.cloud.aws.messaging.listener.SimpleMessageListenerContainer
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration

@Configuration
class AwsConfig() {

    @Bean
    fun amazonS3Builder() = AmazonS3ClientBuilder
        .standard()
        .withRegion(Regions.EU_WEST_2)
        .build()

    @Bean
    fun simpleMessageProcessor() = SimpleMessageListenerContainer()
        .setMaxNumberOfMessages(1)
}