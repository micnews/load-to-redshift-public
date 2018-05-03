package utils

import com.amazonaws.services.sqs.model.SendMessageRequest
import com.amazonaws.services.sqs.AmazonSQSClientBuilder
import com.typesafe.config.ConfigFactory

import scala.collection.JavaConverters._

/**
  * Created by jcorey on 4/6/18.
  */
object SQS {

  val sqs = AmazonSQSClientBuilder.defaultClient()

  val conf = ConfigFactory.load()
  val queueUrl = conf.getString("sqsUrl")

  def addToQueue(fileName: String): Unit ={
    val send_msg_request = new SendMessageRequest()
      .withQueueUrl(queueUrl)
      .withMessageBody(fileName)
    sqs.sendMessage(send_msg_request)
  }

  def addToQueue(fileNames: Seq[String]): Unit ={
    for (file <- fileNames){
      addToQueue(file)
    }
  }

  def pullAllFromQueue(): Seq[String] ={
    sqs.receiveMessage(queueUrl).getMessages.asScala.map(m => {
      sqs.deleteMessage(queueUrl, m.getReceiptHandle)
      m.getBody
    })
  }

}
