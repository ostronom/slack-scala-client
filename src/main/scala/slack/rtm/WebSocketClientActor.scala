package slack.rtm

import java.net.URI
import scala.collection.mutable.{Set ⇒ MSet}
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.{Failure, Success, Try}

import akka.actor.{Actor, ActorLogging, ActorRef, ActorRefFactory, Props, Terminated}
import akka.{Done, NotUsed}
import akka.http.scaladsl.Http
import akka.stream.{ActorMaterializer, OverflowStrategy}
import akka.stream.scaladsl._
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.ws.{Message, TextMessage, WebSocketRequest}

private[rtm] object WebSocketClientActor {
  case class SendWSMessage(message: Message)
  case class RegisterWebsocketListener(listener: ActorRef)
  case class DeregisterWebsocketListener(listener: ActorRef)

  case object WebSocketClientConnected
  case object WebSocketClientDisconnected
  case object WebSocketClientConnectFailed

  case class WebSocketConnectSuccess(queue: SourceQueueWithComplete[Message], closed: Future[Done])
  case object WebSocketConnectFailure
  case object WebSocketDisconnected

  def apply(url: String, listeners: Seq[ActorRef])(implicit arf: ActorRefFactory): ActorRef =
    arf.actorOf(Props(new WebSocketClientActor(url, listeners)))
}

import WebSocketClientActor._

private[rtm] class WebSocketClientActor(url: String, initialListeners: Seq[ActorRef]) extends Actor with ActorLogging {
  implicit val ec = context.dispatcher
  implicit val system = context.system
  implicit val materalizer = ActorMaterializer()

  val listeners = MSet[ActorRef](initialListeners: _*)
  val uri = new URI(url)
  var outboundMessageQueue: Option[SourceQueueWithComplete[Message]] = None

  override def receive = {
    case m: TextMessage ⇒
      log.debug("[WebSocketClientActor] Received Text Message: {}", m)
      sendToListeners(m)
    case m: Message ⇒
      log.debug("[WebsocketClientActor] Received Message: {}", m)
    case SendWSMessage(m) ⇒
      if (outboundMessageQueue.isDefined) {
        outboundMessageQueue.get.offer(m)
      }
    case WebSocketConnectSuccess(queue, closed) ⇒
      log.warning("[WebsocketClientActor] Connection established")
      outboundMessageQueue = Some(queue)
      closed.onComplete(_ ⇒ self ! WebSocketDisconnected)
      sendToListeners(WebSocketClientConnected)
    case WebSocketConnectFailure ⇒
      log.warning("[WebsocketClientActor] Connection failed")
      sendToListeners(WebSocketClientConnectFailed)
    case WebSocketDisconnected ⇒
      log.info("[WebSocketClientActor] WebSocket disconnected.")
      context.stop(self)
    case RegisterWebsocketListener(listener) ⇒
      log.info("[WebSocketClientActor] Registering listener")
      listeners += listener
      context.watch(listener)
    case DeregisterWebsocketListener(listener) ⇒
      listeners -= listener
    case Terminated(actor) ⇒
      listeners -= actor
    case _ ⇒
  }

  def sendToListeners(m: Any): Unit =
    listeners.foreach(_ ! m)

  def connectWebSocket(): Unit = {
    val messageSink: Sink[Message, Future[Done]] = {
      Sink.foreach {
        case message ⇒ self ! message
      }
    }

    val queueSource: Source[Message, SourceQueueWithComplete[Message]] = {
      Source.queue[Message](1000, OverflowStrategy.dropHead)
    }

    val flow: Flow[Message, Message, (Future[Done], SourceQueueWithComplete[Message])] =
      Flow.fromSinkAndSourceMat(messageSink, queueSource)(Keep.both)

    val (upgradeResponse, (closed, messageSourceQueue)) =
      Http().singleWebSocketRequest(WebSocketRequest(url), flow)

    upgradeResponse.onComplete {
      case Success(upgrade) if upgrade.response.status == StatusCodes.SwitchingProtocols ⇒
        log.info("[WebSocketClientActor] Web socket connection success")
        self ! WebSocketConnectSuccess(messageSourceQueue, closed)
      case Success(upgrade) ⇒
        log.info("[WebSocketClientActor] Web socket connection failed: {}", upgrade.response)
        self ! WebSocketConnectFailure
      case Failure(err) ⇒
        log.info("[WebSocketClientActor] Web socket connection failed with error: {}", err.getMessage)
        self ! WebSocketConnectFailure
    }
  }

  override def preStart() = {
    log.info("WebSocketClientActor] Connecting to RTM: {}", url)
    connectWebSocket()
  }

  override def postStop() = {
    outboundMessageQueue.foreach(_.complete)
    log.info("[WebSocketClientActor] Stopping and notifying listeners.")
    sendToListeners(WebSocketClientDisconnected)
  }
}
