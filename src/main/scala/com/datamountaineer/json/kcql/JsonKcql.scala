package com.datamountaineer.json.kcql

import com.datamountaineer.kcql.{Field, Kcql}
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node._

import scala.collection.JavaConversions._
import scala.collection.mutable.{ArrayBuffer, Map => MutableMap}

object JsonKcql {

  implicit class JsonKcqlConverter(val json: JsonNode) extends AnyVal {
    def kcql(query: String): JsonNode = kcql(Kcql.parse(query))

    def kcql(query: Kcql): JsonNode = kcql(query.getFields, !query.hasRetainStructure)

    def kcql()(implicit kcqlContext: KcqlContext): JsonNode = {
      from(json, Vector.empty)
    }

    def kcql(fields: Seq[Field], flatten: Boolean): JsonNode = {
      Option(json).map { _ =>
        fields match {
          case Seq() => json
          case Seq(f) if f.getName == "*" => json
          case _ =>
            if (!flatten) {
              implicit val kcqlContext = new KcqlContext(fields)
              kcql()
            } else {
              kcqlFlatten(fields)
            }
        }
      }.orNull
    }

    def kcqlFlatten(fields: Seq[Field]): JsonNode = {
      def addNode(source: JsonNode, target: ObjectNode, nodeName: String, select: String): Unit = {
        source match {
          case b: BinaryNode => target.put(nodeName, b.binaryValue())
          case b: BooleanNode => target.put(nodeName, b.booleanValue())
          case i: BigIntegerNode => target.put(nodeName, i.bigIntegerValue().longValue())
          case d: DecimalNode => target.put(nodeName, d.decimalValue())
          case d: DoubleNode => target.put(nodeName, d.doubleValue())
          case fl: FloatNode => target.put(nodeName, fl.floatValue())
          case i: IntNode => target.put(nodeName, i.intValue())
          case l: LongNode => target.put(nodeName, l.longValue())
          case s: ShortNode => target.put(nodeName, s.shortValue())
          case t: TextNode => target.put(nodeName, t.textValue())
          case _: NullNode =>
          case _: MissingNode =>
          case other => throw new IllegalArgumentException(s"Invalid path $select")
        }
      }

      fields match {
        case Seq() => json
        case Seq(f) if f.getName == "*" => json
        case _ =>
          json match {
            case node: ObjectNode =>

              val fieldsParentMap = fields.foldLeft(Map.empty[String, ArrayBuffer[String]]) { case (map, f) =>
                val key = Option(f.getParentFields).map(_.mkString(".")).getOrElse("")
                val buffer = map.getOrElse(key, ArrayBuffer.empty[String])
                buffer += f.getName
                map + (key -> buffer)
              }

              val newNode = new ObjectNode(JsonNodeFactory.instance)

              val fieldsNameMap = collection.mutable.Map.empty[String, Int]

              def getNextFieldName(name: String): String = {
                val count = fieldsNameMap.getOrElse(name, 0)
                fieldsNameMap += name -> (count + 1)
                if (count == 0) {
                  name
                } else {
                  s"${name}_$count"
                }
              }

              fields.foreach { f =>
                val sourceNode = {
                  if (!f.hasParents) {
                    Some(node)
                  } else {
                    Option(f.getParentFields).flatMap(p => path(p))
                  }
                }

                val fieldPath = Option(f.getParentFields).map(_.mkString(".") + "." + f.getName).getOrElse(f.getName)

                sourceNode match {
                  case None =>
                  case Some(on: ObjectNode) =>
                    if (f.getName != "*") {
                      Option(on.get(f.getName)).foreach { childNode =>
                        addNode(childNode, newNode, getNextFieldName(f.getAlias), fieldPath)
                      }
                    } else {
                      val key = Option(f.getParentFields).map(_.mkString(".")).getOrElse("")
                      on.fieldNames().filter { name =>
                        fieldsParentMap.get(key).forall(!_.contains(name))
                      }.foreach { name =>
                        addNode(on.get(name), newNode, getNextFieldName(name), fieldPath)
                      }
                    }
                  case Some(other) =>
                    throw new IllegalArgumentException(s"Invalid field selection. '$fieldPath' resolves to node of type:${other.getNodeType}")
                }

              }
              newNode
            case _ => throw new IllegalArgumentException(s"Can't flatten a json type of ${json.getNodeType}")
          }
      }
    }

    def path(path: Seq[String]): Option[JsonNode] = {
      def navigate(node: JsonNode, parents: Seq[String]): Option[JsonNode] = {
        Option(node).flatMap { _ =>
          parents match {
            case head +: tail => navigate(node.get(head), tail)
            case _ => Some(node)
          }
        }
      }

      navigate(json, path)
    }
  }


  private def from(json: JsonNode, parents: Seq[String])(implicit kcqlContext: KcqlContext): JsonNode = {
    def checkFieldsAndReturn() = {
      val fields = kcqlContext.getFieldsForPath(parents)
      require(fields.isEmpty || (fields.size == 1 && fields.head.isLeft && fields.head.left.get.getName == "*"),
        s"You can't select a field from a ${json.getNodeType.toString}.")
      json
    }

    json match {
      case null => null
      case _: BinaryNode => checkFieldsAndReturn()
      case _: BooleanNode => checkFieldsAndReturn()
      case _: BigIntegerNode => checkFieldsAndReturn()
      case _: DecimalNode => checkFieldsAndReturn()
      case _: DoubleNode => checkFieldsAndReturn()
      case _: FloatNode => checkFieldsAndReturn()
      case _: IntNode => checkFieldsAndReturn()
      case _: LongNode => checkFieldsAndReturn()
      case _: ShortNode => checkFieldsAndReturn()
      case _: TextNode => checkFieldsAndReturn()
      case _: NullNode => checkFieldsAndReturn()
      case _: MissingNode => checkFieldsAndReturn()

      case node: ObjectNode => fromObjectNode(node, parents)
      case array: ArrayNode => fromArray(parents, array)
      //case pojoNode:POJONode=>
      case other => throw new IllegalArgumentException(s"Can't apply SQL over node of type:${other.getNodeType}")
    }
  }

  private def fromObjectNode(node: ObjectNode, parents: Seq[String])(implicit kcqlContext: KcqlContext) = {
    val newNode = new ObjectNode(JsonNodeFactory.instance)
    val fields = kcqlContext.getFieldsForPath(parents)
    if (fields.nonEmpty) {
      fields.foreach {
        case Right(parent) => Option(node.get(parent)).foreach { node =>
          newNode.set(parent, from(node, parents :+ parent))
        }
        case Left(parent) if parent.getName == "*" =>
          node.fieldNames().withFilter { f =>
            !fields.exists {
              case Left(field) if field.getName == f => true
              case _ => false
            }
          }.foreach { field => newNode.set(field, from(node.get(field), parents :+ parent.getName)) }

        case Left(parent) => Option(node.get(parent.getName)).foreach { node =>
          newNode.set(parent.getAlias, from(node, parents :+ parent.getName))
        }
      }
    }
    else {
      node.fieldNames()
        .foreach { field =>
          newNode.set(field, from(node.get(field), parents :+ field))
        }
    }
    newNode
  }

  private def fromArray(parents: Seq[String], array: ArrayNode)(implicit kcqlContext: KcqlContext) = {
    if (array.size() == 0) {
      array
    } else {
      val fields = kcqlContext.getFieldsForPath(parents)
      if (fields.size == 1 && fields.head.isLeft && fields.head.left.get.getName == "*") {
        array
      } else {
        val newElements = array.elements().map(from(_, parents)).toList
        new ArrayNode(JsonNodeFactory.instance, newElements)
      }
    }
  }
}
