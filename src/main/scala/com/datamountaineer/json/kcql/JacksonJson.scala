package com.datamountaineer.json.kcql

import com.fasterxml.jackson.annotation.JsonInclude.Include
import com.fasterxml.jackson.databind.{DeserializationFeature, JsonNode, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper

object JacksonJson {
  val mapper = {
    val mapper = new ObjectMapper() with ScalaObjectMapper
    mapper.registerModule(DefaultScalaModule)
    mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
    mapper.setSerializationInclusion(Include.NON_NULL)
    mapper.setSerializationInclusion(Include.NON_EMPTY)
    mapper
  }

  def toJson[T](value: T): String = mapper.writeValueAsString(value)

  def asJson(value:String):JsonNode = mapper.readTree(value)

  def asJson[T](value: T): JsonNode = mapper.valueToTree(value)


  def fromJson[T](json: String)(implicit m: Manifest[T]): T = mapper.readValue[T](json)


  def toMap[V](json: String)(implicit m: Manifest[V]): Map[String, V] = fromJson[Map[String, V]](json)

}

