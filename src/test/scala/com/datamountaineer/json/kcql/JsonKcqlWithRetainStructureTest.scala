package com.datamountaineer.json.kcql

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.IntNode
import org.scalatest.{Matchers, WordSpec}
import JsonKcql._

class JsonKcqlWithRetainStructureTest extends WordSpec with Matchers {

  private def compare[T](actual: JsonNode, t: T) = {
    val expectedRecord = JacksonJson.asJson(t)
    actual.toString shouldBe expectedRecord.toString
  }

  "AvroKcqlExtractor" should {
    "handle null payload" in {
      null.asInstanceOf[JsonNode].kcql("SELECT * FROM topic withstructure") shouldBe null
    }

    "handle Int node" in {
      val container = new IntNode(2000)
      val expected = new IntNode(2000)
      container.kcql("SELECT * FROM topic  withstructure") shouldBe expected
    }


    "throw an exception when trying to select field of an Int node" in {
      val container = new IntNode(20121)
      intercept[IllegalArgumentException] {
        container.kcql("SELECT field1 FROM topic withstructure")
      }
    }

    "handle 'SELECT * FROM topic withstructure' for a record" in {
      val pepperoni = Pizza("pepperoni", Seq(Ingredient("pepperoni", 12, 4.4), Ingredient("onions", 1, 0.4)), false, false, 98)

      val record = JacksonJson.asJson(pepperoni)

      val actual = record.kcql("SELECT *FROM topic withstructure")
      actual shouldBe JacksonJson.fromJson[JsonNode](JacksonJson.toJson(pepperoni))
    }

    "handle 'SELECT *, name as fieldName FROM topic withstructure' for a record" in {
      val pepperoni = Pizza("pepperoni", Seq(Ingredient("pepperoni", 12, 4.4), Ingredient("onions", 1, 0.4)), false, false, 98)
      val record = JacksonJson.asJson(pepperoni)

      val actual = record.kcql("SELECT *, name as fieldName FROM topic  withstructure")

      case class LocalIngredient(name: String, sugar: Double, fat: Double)
      case class LocalPizza(ingredients: Seq[LocalIngredient], vegetarian: Boolean, vegan: Boolean, calories: Int, fieldName: String)

      val newpepperoni = LocalPizza(Seq(LocalIngredient("pepperoni", 12, 4.4), LocalIngredient("onions", 1, 0.4)), false, false, 98, "pepperoni")
      compare(actual, newpepperoni)
    }

    "handle 'SELECT *, ingredients as stuff FROM topic withstructure' for a record" in {
      val pepperoni = Pizza("pepperoni", Seq(Ingredient("pepperoni", 12, 4.4), Ingredient("onions", 1, 0.4)), false, false, 98)
      val record = JacksonJson.asJson(pepperoni)

      val actual = record.kcql("SELECT *, ingredients as stuff FROM topic withstructure")

      case class LocalIngredient(name: String, sugar: Double, fat: Double)
      case class LocalPizza(name: String, vegetarian: Boolean, vegan: Boolean, calories: Int, stuff: Seq[LocalIngredient])

      val newpepperoni = LocalPizza(pepperoni.name, pepperoni.vegetarian, pepperoni.vegan, pepperoni.calories, Seq(LocalIngredient("pepperoni", 12, 4.4), LocalIngredient("onions", 1, 0.4)))
      compare(actual, newpepperoni)
    }

    "handle 'SELECT name as fieldName, * FROM topic withstructure' for a record" in {
      val pepperoni = Pizza("pepperoni", Seq(Ingredient("pepperoni", 12, 4.4), Ingredient("onions", 1, 0.4)), false, false, 98)
      val record = JacksonJson.asJson(pepperoni)

      val actual = record.kcql("SELECT name as fieldName, * FROM topic withstructure")

      case class LocalIngredient(name: String, sugar: Double, fat: Double)
      case class LocalPizza(fieldName: String, ingredients: Seq[LocalIngredient], vegetarian: Boolean, vegan: Boolean, calories: Int)
      val newpepperoni = LocalPizza(pepperoni.name, Seq(LocalIngredient("pepperoni", 12, 4.4), LocalIngredient("onions", 1, 0.4)), pepperoni.vegetarian, pepperoni.vegan, pepperoni.calories)
      compare(actual, newpepperoni)
    }

    "handle 'SELECT vegan FROM topic withstructure' for a record" in {
      val pepperoni = Pizza("pepperoni", Seq(Ingredient("pepperoni", 12, 4.4), Ingredient("onions", 1, 0.4)), false, false, 98)
      val record = JacksonJson.asJson(pepperoni)

      val actual = record.kcql("SELECT vegan FROM topic withstructure")

      case class LocalPizza(vegan: Boolean)
      val newpepperoni = LocalPizza(pepperoni.vegan)
      compare(actual, newpepperoni)
    }

    "handle 'SELECT vegan as veganA FROM topic withstructure' for a record" in {
      val pepperoni = Pizza("pepperoni", Seq(Ingredient("pepperoni", 12, 4.4), Ingredient("onions", 1, 0.4)), false, false, 98)
      val record = JacksonJson.asJson(pepperoni)

      val actual = record.kcql("SELECT vegan as veganA FROM topic withstructure")

      case class LocalPizza(veganA: Boolean)
      val newpepperoni = LocalPizza(pepperoni.vegan)
      compare(actual, newpepperoni)
    }

    "handle 'SELECT ingredients.name FROM topic withstructure' for a record" in {
      val pepperoni = Pizza("pepperoni", Seq(Ingredient("pepperoni", 12, 4.4), Ingredient("onions", 1, 0.4)), false, false, 98)
      val record = JacksonJson.asJson(pepperoni)

      val actual = record.kcql("SELECT ingredients.name FROM topic withstructure")

      case class LocalIngredient(name: String)
      case class LocalPizza(ingredients: Seq[LocalIngredient])
      val newpepperoni = LocalPizza(Seq(LocalIngredient("pepperoni"), LocalIngredient("onions")))
      compare(actual, newpepperoni)
    }

    "handle 'SELECT ingredients.name, ingredients.sugar FROM topic' for a record withstructure" in {
      val pepperoni = Pizza("pepperoni", Seq(Ingredient("pepperoni", 12, 4.4), Ingredient("onions", 1, 0.4)), false, false, 98)
      val record = JacksonJson.asJson(pepperoni)

      val actual = record.kcql("SELECT ingredients.name, ingredients.sugar FROM topic withstructure")

      case class LocalIngredient(name: String, sugar: Double)
      case class LocalPizza(ingredients: Seq[LocalIngredient])
      val newpepperoni = LocalPizza(Seq(LocalIngredient("pepperoni", 12), LocalIngredient("onions", 1)))
      compare(actual, newpepperoni)
    }

    "handle 'SELECT ingredients.name as fieldName, ingredients.sugar as fieldSugar FROM topic withstructure' for a record" in {
      val pepperoni = Pizza("pepperoni", Seq(Ingredient("pepperoni", 12, 4.4), Ingredient("onions", 1, 0.4)), false, false, 98)
      val record = JacksonJson.asJson(pepperoni)

      val actual = record.kcql("SELECT ingredients.name as fieldName, ingredients.sugar as fieldSugar FROM topic withstructure")

      case class LocalIngredient(fieldName: String, fieldSugar: Double)
      case class LocalPizza(ingredients: Seq[LocalIngredient])
      val newpepperoni = LocalPizza(Seq(LocalIngredient("pepperoni", 12), LocalIngredient("onions", 1)))
      compare(actual, newpepperoni)
    }


    "handle 'SELECT ingredients.*,ingredients.name as fieldName, ingredients.sugar as fieldSugar FROM topic withstructure' for a record" in {
      val pepperoni = Pizza("pepperoni", Seq(Ingredient("pepperoni", 12, 4.4), Ingredient("onions", 1, 0.4)), false, false, 98)
      val record = JacksonJson.asJson(pepperoni)

      val actual = record.kcql("SELECT ingredients.*,ingredients.name as fieldName, ingredients.sugar as fieldSugar FROM topic withstructure")
      case class LocalIngredient(fat: Double, fieldName: String, fieldSugar: Double)
      case class LocalPizza(ingredients: Seq[LocalIngredient])
      val newpepperoni = LocalPizza(Seq(LocalIngredient(4.4, "pepperoni", 12), LocalIngredient(0.4, "onions", 1)))
      compare(actual, newpepperoni)
    }

    "handle 'SELECT ingredients.name as fieldName,ingredients.*, ingredients.sugar as fieldSugar FROM topic' for a record" in {
      val pepperoni = Pizza("pepperoni", Seq(Ingredient("pepperoni", 12, 4.4), Ingredient("onions", 1, 0.4)), false, false, 98)
      val record = JacksonJson.asJson(pepperoni)

      val actual = record.kcql("SELECT ingredients.name as fieldName,ingredients.*, ingredients.sugar as fieldSugar FROM topic withstructure")
      case class LocalIngredient(fieldName: String, fat: Double, fieldSugar: Double)
      case class LocalPizza(ingredients: Seq[LocalIngredient])
      val newpepperoni = LocalPizza(Seq(LocalIngredient("pepperoni", 4.4, 12), LocalIngredient("onions", 0.4, 1)))
      compare(actual, newpepperoni)
    }

    "handle 'SELECT ingredients.name as fieldName, ingredients.sugar as fieldSugar, ingredients.* FROM topic' for a record" in {
      val pepperoni = Pizza("pepperoni", Seq(Ingredient("pepperoni", 12, 4.4), Ingredient("onions", 1, 0.4)), false, false, 98)
      val record = JacksonJson.asJson(pepperoni)

      val actual = record.kcql("SELECT ingredients.name as fieldName, ingredients.sugar as fieldSugar, ingredients.* FROM topic withstructure")

      case class LocalIngredient(fieldName: String, fieldSugar: Double, fat: Double)
      case class LocalPizza(ingredients: Seq[LocalIngredient])
      val newpepperoni = LocalPizza(Seq(LocalIngredient("pepperoni", 12, 4.4), LocalIngredient("onions", 1, 0.4)))
      compare(actual, newpepperoni)
    }


    "handle 'SELECT name, ingredients.name as fieldName, ingredients.sugar as fieldSugar, ingredients.* FROM topic withstructure' for a record" in {
      val pepperoni = Pizza("pepperoni", Seq(Ingredient("pepperoni", 12, 4.4), Ingredient("onions", 1, 0.4)), false, false, 98)

      val record = JacksonJson.asJson(pepperoni)
      val actual = record.kcql("SELECT name, ingredients.name as fieldName, ingredients.sugar as fieldSugar, ingredients.* FROM topic withstructure")

      case class LocalIngredient(fieldName: String, fieldSugar: Double, fat: Double)
      case class LocalPizza(name: String, ingredients: Seq[LocalIngredient])
      val newpepperoni = LocalPizza("pepperoni", Seq(LocalIngredient("pepperoni", 12, 4.4), LocalIngredient("onions", 1, 0.4)))
      compare(actual, newpepperoni)
    }

    "handle 'SELECT name, ingredients.name as fieldName, ingredients.sugar as fieldSugar, ingredients.*, calories as cals FROM topic withstructure' for a record" in {
      val pepperoni = Pizza("pepperoni", Seq(Ingredient("pepperoni", 12, 4.4), Ingredient("onions", 1, 0.4)), false, false, 98)
      val record = JacksonJson.asJson(pepperoni)

      val actual = record.kcql("SELECT name, ingredients.name as fieldName, ingredients.sugar as fieldSugar, ingredients.*, calories as cals FROM topic withstructure")
      case class LocalIngredient(fieldName: String, fieldSugar: Double, fat: Double)
      case class LocalPizza(name: String, ingredients: Seq[LocalIngredient], cals: Int)
      val newpepperoni = LocalPizza("pepperoni", Seq(LocalIngredient("pepperoni", 12, 4.4), LocalIngredient("onions", 1, 0.4)), 98)
      compare(actual, newpepperoni)
    }

    "handle 'SELECT name, ingredients.name as fieldName, calories as cals,ingredients.sugar as fieldSugar, ingredients.* FROM topic withstructure' for a record" in {
      val pepperoni = Pizza("pepperoni", Seq(Ingredient("pepperoni", 12, 4.4), Ingredient("onions", 1, 0.4)), false, false, 98)
      val record = JacksonJson.asJson(pepperoni)

      val actual = record.kcql("SELECT name, ingredients.name as fieldName, calories as cals, ingredients.sugar as fieldSugar, ingredients.* FROM topic withstructure")
      case class LocalIngredient(fieldName: String, fieldSugar: Double, fat: Double)
      case class LocalPizza(name: String, ingredients: Seq[LocalIngredient], cals: Int)
      val newpepperoni = LocalPizza("pepperoni", Seq(LocalIngredient("pepperoni", 12, 4.4), LocalIngredient("onions", 1, 0.4)), 98)
      compare(actual, newpepperoni)
    }
  }
}
