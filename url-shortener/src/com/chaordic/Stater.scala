package com.chaordic

import akka.actor._
import com.typesafe.config.ConfigFactory
import org.apache.commons.validator.routines.UrlValidator
import spray.http.StatusCodes
import spray.routing._
import scala.util.Random
import com.chaordic.cassandra.CassandraClient
import com.chaordic.shortener.Shortner
import scalaz.concurrent._
import spray.json._
import spray.httpx.SprayJsonSupport
import spray.json.DefaultJsonProtocol
import spray.httpx.unmarshalling.Unmarshaller

case class OriginalUrl(longUrl: String)

object OriginalUrlJsonSupport extends DefaultJsonProtocol with SprayJsonSupport {
  println("inside the path 3")
  implicit val OriginalUrlFormats = jsonFormat1(OriginalUrl)
}

object Stater extends App with SimpleRoutingApp {

  val URLConstant = "http://chrdc.co/"
  val client = new CassandraClient()

  implicit val system = ActorSystem("url-shortener")
  import OriginalUrlJsonSupport._

  startServer("0.0.0.0", 8081) {
    post {
      entity(as[OriginalUrl]) { url =>
        createShortUrl(url)
      }
    } ~
      get {
        path(Segment) {
          (id) =>
            {
              retrieveUrl(id.toString())
            }
        }
      }
  }

  def createShortUrl(path: OriginalUrl) = (ctx: RequestContext) => {

    var random = ""
    Task {

      val validator = new UrlValidator(List("http", "https").toArray)
      if (validator.isValid(path.longUrl)) {

        do{
          random = Random.alphanumeric.take(7).mkString
          random
        }while(!client.exists(random))

        //client.add(random, URLConstant + random, path.longUrl)

      } else {
        throw new Exception("The supplied url is invalid.")
      }
    }.runAsync(_.fold(l => ctx.reject(
      ValidationRejection("Invalid url provided", Some(l))),
      r => ctx.complete(s"" + URLConstant + "/" + random)))
  }

  def retrieveOriginalUrl(path: String): String = {
    client.getUrl(path)
  }

  def retrieveUrl(id: String) = (ctx: RequestContext) => {

    var resp = ""

    Task {
      
      //resp = client.getUrl(id)

      printf(resp)

    }.runAsync(_.fold(l => ctx.reject(
      ValidationRejection("Invalid id provided", Some(l))),
      r => ctx.complete(resp)))
  }
}