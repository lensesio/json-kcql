package com.datamountaineer.json.kcql

import com.datamountaineer.json.kcql.JsonKcql._
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.{ObjectNode, TextNode}
import org.scalatest.{Matchers, WordSpec}

class JsonKcqTest extends WordSpec with Matchers {

  private def compare[T](actual: JsonNode, t: T) = {
    val expected = JacksonJson.asJson(t)
    actual.toString shouldBe expected.toString
  }

  "AvroKcqlExtractor" should {
    "read json" in {
      val json =
        """
          |{
          |  "f1":"v1",
          |  "f2": 21
          |}
        """.stripMargin
      JacksonJson.asJson(json) match {
        case _:TextNode=> fail("")
        case _:ObjectNode=> true shouldBe true
      }
    }

    "handle null payload" in {
      null.asInstanceOf[JsonNode].kcql("SELECT * FROM topic") shouldBe null.asInstanceOf[Any]
    }


    "handle 'SELECT name,vegan, calories  FROM topic' for a record" in {
      val pepperoni = Pizza("pepperoni", Seq(Ingredient("pepperoni", 12, 4.4), Ingredient("onions", 1, 0.4)), false, false, 98)
      val actual = JacksonJson.asJson(pepperoni).kcql("SELECT name,vegan, calories FROM topic")

      case class LocalPizza(name: String, vegan: Boolean, calories: Int)
      val expected = LocalPizza(pepperoni.name, pepperoni.vegan, pepperoni.calories)

      compare(actual, expected)
    }

    "handle 'SELECT name as fieldName,vegan as V, calories as C FROM topic' for a record" in {
      val pepperoni = Pizza("pepperoni", Seq(Ingredient("pepperoni", 12, 4.4), Ingredient("onions", 1, 0.4)), false, false, 98)
      val actual = JacksonJson.asJson(pepperoni).kcql("SELECT name as fieldName,vegan as V, calories as C FROM topic")

      case class LocalPizza(fieldName: String, V: Boolean, C: Int)
      val expected = LocalPizza(pepperoni.name, pepperoni.vegan, pepperoni.calories)

      compare(actual, expected)
    }

    "handle 'SELECT calories as C ,vegan as V ,name as fieldName FROM topic' for a record" in {
      val pepperoni = Pizza("pepperoni", Seq(Ingredient("pepperoni", 12, 4.4), Ingredient("onions", 1, 0.4)), false, false, 98)
      val actual = JacksonJson.asJson(pepperoni).kcql("SELECT  calories as C,vegan as V,name as fieldName FROM topic")

      case class LocalPizza(C: Int, V: Boolean, fieldName: String)
      val expected = LocalPizza(pepperoni.calories, pepperoni.vegan, pepperoni.name)

      compare(actual, expected)
    }

    "throw an exception when selecting arrays ' for a record" in {
      val pepperoni = Pizza("pepperoni", Seq(Ingredient("pepperoni", 12, 4.4), Ingredient("onions", 1, 0.4)), false, false, 98)
      intercept[IllegalArgumentException] {
        JacksonJson.asJson(pepperoni).kcql("SELECT *, name as fieldName FROM topic")
      }
    }

    "handle 'SELECT name, address.street.name FROM topic' from record" in {
      val person = Person("Rick", Address(Street("Rock St"), None, "MtV", "CA", "94041", "USA"))
      val actual = JacksonJson.asJson(person).kcql("SELECT name, address.street.name FROM topic")

      case class LocalPerson(name: String, name_1: String)
      val localPerson = LocalPerson(person.name, person.address.street.name)
      compare(actual, localPerson)
    }

    "handle 'SELECT name, address.street.name as streetName FROM topic' from record" in {
      val person = Person("Rick", Address(Street("Rock St"), None, "MtV", "CA", "94041", "USA"))
      val actual = JacksonJson.asJson(person).kcql("SELECT name, address.street.name as streetName FROM topic")

      case class LocalPerson(name: String, streetName: String)
      val localPerson = LocalPerson(person.name, person.address.street.name)
      compare(actual, localPerson)
    }

    "handle 'SELECT name, address.street.name as streetName, address.street2.name as streetName2 FROM topic' from record" in {
      val person = Person("Rick", Address(Street("Rock St"), None, "MtV", "CA", "94041", "USA"))
      val actual = JacksonJson.asJson(person).kcql("SELECT name, address.street.name as streetName, address.street2.name as streetName2 FROM topic")

      case class LocalPerson(name: String, streetName: String, streetName2: String)
      val localPerson = LocalPerson(person.name, person.address.street.name, person.address.street2.map(_.name).orNull)
      compare(actual, localPerson)
    }

    "handle 'SELECT name, address.street.*, address.street2.name as streetName2 FROM topic' from record" in {
      val person = Person("Rick", Address(Street("Rock St"), None, "MtV", "CA", "94041", "USA"))
      val actual = JacksonJson.asJson(person).kcql("SELECT name, address.street.*, address.street2.name as streetName2 FROM topic")

      case class LocalPerson(name: String, name_1: String, streetName2: String)
      val localPerson = LocalPerson(person.name, person.address.street.name, person.address.street2.map(_.name).orNull)
      compare(actual, localPerson)
    }

    "handle 'SELECT name, address.street.*, address.street2.* FROM topic' from record" in {
      val person = Person("Rick", Address(Street("Rock St"), None, "MtV", "CA", "94041", "USA"))
      val actual = JacksonJson.asJson(person).kcql("SELECT name, address.street.*, address.street2.* FROM topic")

      case class LocalPerson(name: String, name_1: String, name_2: String)
      val localPerson = LocalPerson(person.name, person.address.street.name, person.address.street2.map(_.name).orNull)
      compare(actual, localPerson)

      val actual1 = JacksonJson.asJson(person).kcql("SELECT name, address.street.*, address.street2.* FROM topic")
      val localPerson1 = LocalPerson(person.name, person.address.street.name, person.address.street2.map(_.name).orNull)
      compare(actual1, localPerson1)

    }

    "handle 'SELECT address.state, address.city,name, address.street.name FROM topic' from record" in {
      val person = Person("Rick", Address(Street("Rock St"), None, "MtV", "CA", "94041", "USA"))
      val actual = JacksonJson.asJson(person).kcql("SELECT address.state, address.city,name, address.street.name FROM topic")

      case class LocalPerson(state: String, city: String, name: String, name_1: String)
      val localPerson = LocalPerson(person.address.state, person.address.city, person.name, person.address.street.name)
      compare(actual, localPerson)
    }

    "handle 'SELECT address.state as S, address.city as C,name, address.street.name FROM topic' from record" in {
      val person = Person("Rick", Address(Street("Rock St"), None, "MtV", "CA", "94041", "USA"))
      val actual = JacksonJson.asJson(person).kcql("SELECT address.state as S, address.city as C,name, address.street.name FROM topic")

      case class LocalPerson(S: String, C: String, name: String, name_1: String)
      val localPerson = LocalPerson(person.address.state, person.address.city, person.name, person.address.street.name)
      compare(actual, localPerson)
    }

    "not throw an exception if the field doesn't exist in the schema" in {
      val person = Person("Rick", Address(Street("Rock St"), Some(Street("Sunset Boulevard")), "MtV", "CA", "94041", "USA"))
      JacksonJson.asJson(person).kcql("SELECT address.bam, address.city,name, address.street.name FROM topic")
    }


    "handle 'SELECT * FROM simpleAddress' from record" in {
      val address = SimpleAddress("Rock St", "MtV", "CA", "94041", "USA")
      val actual = JacksonJson.asJson(address).kcql("SELECT * FROM simpleAddress")
      compare(actual, address)
    }

    "handle 'SELECT street as S, city, state, zip as Z, country as C  FROM simpleAddress' from record" in {
      val address = SimpleAddress("Rock St", "MtV", "CA", "94041", "USA")
      val actual = JacksonJson.asJson(address).kcql("SELECT street as S, city, state, zip as Z, country as C  FROM simpleAddress")

      case class LocalSimpleAddress(S: String, city: String, state: String, Z: String, C: String)
      val expected = LocalSimpleAddress(address.street, address.city, address.state, address.zip, address.country)

      compare(actual, expected)
    }

    "handle 'SELECT  zip as Z, * FROM simpleAddress' from record" in {
      val address = SimpleAddress("Rock St", "MtV", "CA", "94041", "USA")
      val actual = JacksonJson.asJson(address).kcql("SELECT zip as Z, * FROM simpleAddress")

      case class LocalSimpleAddress(Z: String, street: String, city: String, state: String, country: String)
      val expected = LocalSimpleAddress(address.zip, address.street, address.city, address.state, address.country)

      compare(actual, expected)
    }

    "handle 'SELECT  zip as Z, *, state as S FROM simpleAddress' from record" in {
      val address = SimpleAddress("Rock St", "MtV", "CA", "94041", "USA")
      val actual = JacksonJson.asJson(address).kcql("SELECT zip as Z, *, state as S FROM simpleAddress")

      case class LocalSimpleAddress(Z: String, street: String, city: String, country: String, S: String)
      val expected = LocalSimpleAddress(address.zip, address.street, address.city, address.country, address.state)

      compare(actual, expected)
    }
  }
}
