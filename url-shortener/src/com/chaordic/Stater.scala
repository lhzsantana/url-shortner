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
import com.chaordic.cassandra.ShortenedUrl

case class OriginalUrl(longUrl: String)
case class NewUrl(longUrl: String, id: String, shortUrl: String)

object OriginalUrlJsonSupport extends DefaultJsonProtocol with SprayJsonSupport {
  implicit val OriginalUrlFormats = jsonFormat1(OriginalUrl)
  implicit val NewUrlFormats = jsonFormat3(NewUrl)
  implicit val ShortenedUrlFormats = jsonFormat4(ShortenedUrl)
}

object Stater extends App with SimpleRoutingApp {

  val URLConstant = "http://chrdc.co/"
  val client = new CassandraClient()

  implicit val system = ActorSystem("url-shortener")
  import OriginalUrlJsonSupport._

  startServer("0.0.0.0", 8081) {
    client.connect("localhost")
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

    var resp = "";

    Task {

      val validator = new UrlValidator(List("http", "https").toArray)
      if (validator.isValid(path.longUrl)) {

        var original = client.getOriginal(path.longUrl)

        if (original == null) {

          var random = ""
          do {
            random = Random.alphanumeric.take(7).mkString
          } while (client.existsId(random))

          client.add(random, URLConstant + random, path.longUrl)
          
          resp = (new NewUrl(path.longUrl, random, URLConstant + random)).toJson.toString()
        }else{
          resp = original.toJson.toString()
        }
      } else {
        throw new Exception("The supplied url is invalid.")
      }
    }.runAsync(_.fold(l => ctx.reject(
      ValidationRejection("Invalid url provided", Some(l))),
      r => ctx.complete(resp)))
  }

  def retrieveUrl(id: String) = (ctx: RequestContext) => {

    var resp = ""

    Task {

      resp = client.getUrl(id).toJson.toString()

      printf(resp)

    }.runAsync(_.fold(l => ctx.reject(
      ValidationRejection("Invalid id provided", Some(l))),
      r => ctx.complete(resp)))
  }
}