package com.chaordic.cassandra

import com.datastax.driver.core.Cluster
import com.datastax.driver.core.ResultSet
import com.datastax.driver.core.Row
import com.datastax.driver.core.Session
import com.chaordic.NewUrl

case class ShortenedUrl(longUrl: String, id: String, shortUrl: String, status: String)

class CassandraClient {

  var client: Cluster = null
  private var session: Session = null

  def connect(node: String) {

    printf("Connecting to " + node)

    client = Cluster.builder().addContactPoint(node).build()

    val metadata = client.getMetadata()

    printf("Connected to cluster: %s\n", metadata.getClusterName())

    try {
      client.newSession().execute("CREATE KEYSPACE chaord WITH replication " +
        "= {'class':'SimpleStrategy', 'replication_factor':1};")

      client.newSession().execute("CREATE TABLE chaord.urls (id text PRIMARY KEY, " +
        " original text, short text)")

      client.newSession().execute("CREATE INDEX original ON chaord.urls (original)");
    } catch {
      case e: Exception => {}
    }
  }

  def close1() {
    client.close()
  }

  def add(id: String, shortUrl: String, originalUrl: String) {

    println("Adding the a new url")

    val cql = "INSERT INTO chaord.urls (id, original, short)" +
      "VALUES('" + id + "','" + originalUrl + "','" + shortUrl + "')"

    printf(cql)

    client.newSession().execute(cql)
  }

  def getOriginal(url: String): NewUrl = {

    println("Getting original " + url)

    val cql = "SELECT * FROM chaord.urls WHERE original='" + url + "'"

    println(cql)

    try {

      val rs = client.newSession().execute(cql)

      val iter = rs.iterator()

      if (iter.hasNext()) {
        val row = iter.next()
        
        return (new NewUrl(row.getString("original"), row.getString("id"), row.getString("short")))
      }

    } catch {
      case e: Exception => {
        println("Error " + e.getMessage + e.getStackTrace)
      }
    }

    return null
  }

  def getId(id: String): ResultSet = {
    return client.newSession().execute("SELECT * FROM chaord.urls WHERE id='" + id + "'")
  }

  def existsId(id: String): Boolean = {

    println("Checking if it exists " + id)

    return getId(id).all().size() > 0
  }

  def getUrl(id: String): ShortenedUrl = {

    println("Get url")

    var rs = getId(id)

    val row = rs.all().get(0)

    return new ShortenedUrl(row.getString("original"), row.getString("id"), row.getString("short"), "OK")
  }
}