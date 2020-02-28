package com.example.kotlinaws.steve

data class AwsS3ListObjectResponse(
    val bucketName: String = "N/A",
    val files: List<String> = emptyList()
)