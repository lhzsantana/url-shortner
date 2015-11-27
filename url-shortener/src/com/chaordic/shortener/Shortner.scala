package com.chaordic.shortener;

import org.apache.commons.validator.routines.UrlValidator
import scala.util.Random

object Shortner {

  def createShortUrl(path: String): String = {
    val validator = new UrlValidator(List("http", "https").toArray)
    if (validator.isValid(path)) {
      return Random.alphanumeric.take(7).mkString
    } else {
      throw new Exception("The supplied url is invalid.")
    }
  }

}