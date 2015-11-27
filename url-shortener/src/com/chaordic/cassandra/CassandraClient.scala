package com.chaordic.cassandra

import com.datastax.driver.core.Cluster
import com.datastax.driver.core.ResultSet
import com.datastax.driver.core.Session

class CassandraClient {

  var client: Cluster = null
  private var session: Session = null

  def connect(node: String) {
    client = Cluster.builder().addContactPoint(node).build()

    val metadata = client.getMetadata()

    printf("Connected to cluster: %s\n", metadata.getClusterName())

    try {
      client.newSession().execute("CREATE KEYSPACE chaordic1 WITH replication " +
        "= {'class':'SimpleStrategy', 'replication_factor':1};")

      client.newSession().execute("CREATE TABLE chaordic1.urls (id text PRIMARY KEY, " +
        " original text, short text)")
    } catch {
      case e: Exception => {}
    }
  }

  def close1() {
    client.close()
  }

  def add(id: String, shortUrl: String, originalUrl: String) {

    val cql = "INSERT INTO chaordic1.urls (id, original, short)" +
      "VALUES('" + id + "','" + originalUrl + "','" + shortUrl + "')"

    //printf(cql)

    client.newSession().execute(cql)
  }

  def getId(id: String): ResultSet = {
    return client.newSession().execute("SELECT * FROM chaordic1.urls WHERE id='" + id + "'")
  }

  def exists(id: String): Boolean = {
    return getId(id).all().size() > 0
  }

  def getUrl(id: String): String = {

    var rs = getId(id)

    val list = rs.all()

    return "" + list.get(0);
  }
}