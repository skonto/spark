/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.deploy


import java.io.{ByteArrayInputStream, InputStreamReader}
import java.net.HttpURLConnection
import java.net.URL
import java.nio.charset.Charset
import java.security.MessageDigest
import java.util.UUID

import collection.JavaConverters._
import org.apache.commons.io.{Charsets, IOUtils}

object DCOS_VERSION extends Enumeration {
  type DCOS_VERSION = Value
  val v1_10_X, v1_11_X = Value
}

private[spark] object DCOSSecretStoreUtils {

  import org.apache.spark.deploy.DCOS_VERSION._

  /**
   * Writes a binary secret to the DC/OS secret store.
   * To be used by Spark Submit, integrates with its printstream
   *
   *  @param hostName the hostname of the DC/OS master
   *  @param path {secret_store_name}/path/to/secret
   *  @param value secret value in binary format
   *  @param token the authentication token to use for accessing the secrets HTTP API
   *  @param dcosVersion DC/OS version. HTTP API depends on that.
   */
  def writeBinarySecret(
      hostName: String,
      path: String,
      value: Array[Byte],
      token: String,
      secretStoreName: String,
      dcosVersion: DCOS_VERSION = v1_10_X): Unit = {
    val authurl = new URL(s"https://$hostName/secrets/v1/secret/$secretStoreName" + path)
    val conn = authurl.openConnection().asInstanceOf[HttpURLConnection]
    conn.setRequestProperty("Authorization", s"token=$token")
    conn.setRequestMethod("PUT")
    conn.setDoOutput(true)

    dcosVersion match {
      case DCOS_VERSION.`v1_10_X` =>
        conn.setRequestProperty("Content-Type", "application/json")

        val encoder = java.util.Base64.getEncoder()
        val outputValue = encoder.encode(value)

        val outPut =
          s"""
             |{"value":"${new String(outputValue)}"}
          """.stripMargin
        val byteArrayOutput = outPut.getBytes(Charset.forName("UTF-8"))
        conn.getOutputStream.write(byteArrayOutput)
        conn.getOutputStream.flush()
        conn.getOutputStream.close()
        val res = IOUtils.toString(conn.getInputStream())
        // scalastyle:off println
        SparkSubmit.printStream.println(s"Res:${conn.getResponseMessage}")
        // scalastyle:on println
        conn.disconnect()

      case DCOS_VERSION.`v1_11_X` =>
        conn.setRequestProperty("Content-Type", "application/octet-stream")
        conn.getOutputStream.write(value)
        conn.getOutputStream.flush()
        conn.getOutputStream.close()
        val res = IOUtils.toString(conn.getInputStream())
        // scalastyle:off println
        SparkSubmit.printStream.println(s"Res:${conn.getResponseMessage}")
        // scalastyle:on println
        conn.disconnect()
      }
    }

  def bytesToHex(bytes: Array[Byte]): String = {
    val builder = StringBuilder.newBuilder
    for (b <- bytes) {
      builder.append("%02x".format(b))
    }
    builder.toString
  }

  def getSecretName(): String = {
    val salt = MessageDigest.getInstance("SHA-256")
    salt.update(UUID.randomUUID().toString().getBytes("UTF-8"))
    val digest = bytesToHex(salt.digest())
    s"DTS-$digest"
  }

  def formatPath(secretPath: String, secretName: String, version: DCOS_VERSION): String = {
    val prefix = {
      version match {
        case DCOS_VERSION.`v1_10_X` => "__dcos_base64__"
        case DCOS_VERSION.`v1_11_X` => ""
      }
    }
     secretPath + "/" + prefix + secretName
  }
}
