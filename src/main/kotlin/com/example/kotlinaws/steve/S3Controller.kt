package com.example.kotlinaws.steve

import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.model.CompressionType
import com.amazonaws.services.s3.model.ExpressionType
import com.amazonaws.services.s3.model.InputSerialization
import com.amazonaws.services.s3.model.JSONInput
import com.amazonaws.services.s3.model.JSONOutput
import com.amazonaws.services.s3.model.JSONType
import com.amazonaws.services.s3.model.OutputSerialization
import com.amazonaws.services.s3.model.S3ObjectSummary
import com.amazonaws.services.s3.model.SelectObjectContentEventVisitor
import com.amazonaws.services.s3.model.SelectObjectContentRequest
import com.amazonaws.services.s3.model.SelectObjectContentResult
import com.amazonaws.services.sqs.AmazonSQSAsync
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.cloud.aws.messaging.listener.annotation.SqsListener
import org.springframework.stereotype.Controller
import org.springframework.stereotype.Service
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.RequestParam
import org.springframework.web.bind.annotation.ResponseBody
import java.util.Random
import java.util.UUID
import javax.annotation.PostConstruct
import javax.annotation.PreDestroy

private val inputSerialization = InputSerialization()
    .withJson(JSONInput().withType(JSONType.DOCUMENT))
    .withCompressionType(CompressionType.NONE)

private val outputSerialization = OutputSerialization()
    .withJson(JSONOutput())

@Controller
class AWSController(val amazonS3: AmazonS3, val amazonSQSAsync: AmazonSQSAsync) {

    @ResponseBody
    @GetMapping("/get-bucket-info")
    fun getBucketInformation(@RequestParam("bucket-name") bucketName: String): AwsS3ListObjectResponse {
        return if (amazonS3.doesBucketExistV2(bucketName)) {
            //Find all S3Objects
            val objectListing: List<S3ObjectSummary> =
                amazonS3.listObjects(bucketName).objectSummaries

            objectListing.asSequence().map { s3Object ->
                 SelectObjectContentRequest()
                    .withBucketName(bucketName)
                    .withKey(s3Object.key)
                    .withExpression("SELECT s.name FROM S3Object s WHERE s.HomeGoals >= 9")
                    .withExpressionType(ExpressionType.SQL)
                    .withInputSerialization(inputSerialization)
                    .withOutputSerialization(outputSerialization)
            }.map { contentRequest ->
                amazonS3.selectObjectContent(contentRequest)
            }.map { contentResult ->
                extract(contentResult)
            }.filterNot { it.isEmpty()
            }.forEach { file ->

                println(file)
                amazonSQSAsync.sendMessage("https://sqs.eu-west-2.amazonaws.com/783379042518/football-queue", file)

            }

            AwsS3ListObjectResponse()
        } else {
            AwsS3ListObjectResponse()
        }
    }

    fun extract(s3SelectObject: SelectObjectContentResult): String {
        val inputStream = s3SelectObject.payload?.getRecordsInputStream(object : SelectObjectContentEventVisitor() {})

        return inputStream?.bufferedReader()?.readText() ?: "NOPE"
    }
}

@Service
class MyProcessor() {

    @SqsListener("football-queue")
    fun processMessage(payload: String) {
        println("MY PAYLOAD OFF SQS: $payload")
    }

}

@Service
@ConditionalOnProperty("data.populator")
class DataPopulator(val amazonS3: AmazonS3) {

    val listOfFilesIManage = mutableListOf<String>()

    @PostConstruct
    fun initializeData() {
        amazonS3.createBucket("steves-amazing-data-test")
        println("I am sending data..")

        IntRange(0, 500).forEach {

            val homeGoals = Random().nextInt(10)
            val awayGoals = Random().nextInt(10)

            val fileId = UUID.randomUUID()
            val json = """
            {
            "name": "GameSteve-${fileId}",
            "HomeGoals": ${homeGoals},
            "AwayGoals": ${awayGoals}
            }
        """.trimIndent()

            amazonS3.putObject("steves-amazing-data-test", "${fileId}.json", json)

            listOfFilesIManage.add(fileId.toString())
        }
    }

    @PreDestroy
    fun cleanUpData() {
        listOfFilesIManage.forEach {
            amazonS3.deleteObject("steves-amazing-data-test", "${it}.json")
        }

        println("All objects deleted")
        println("Deleting bucket.")
        amazonS3.deleteBucket("steves-amazing-data-test")
        println("Bucket deleted...")
    }

}