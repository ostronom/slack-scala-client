package slack

import akka.actor._
import slack.rtm.SlackRtmClient

import scala.concurrent.duration._

object Main extends App {
  val token = "..."
  val endpoint = "..."
  implicit val system = ActorSystem("slack")
  implicit val ec = system.dispatcher

  val client = SlackRtmClient(endpoint, token)
  val selfId = client.state.self.id

  client.onEvent { event ⇒
    system.log.info("Received new event: {}", event)
  /*
    val mentionedIds = SlackUtil.extractMentionedIds(message.text)

    if (mentionedIds.contains(selfId)) {
      client.sendMessage(message.channel, s"<@${message.user}>: Hey!")
    }
   */
  }
}
