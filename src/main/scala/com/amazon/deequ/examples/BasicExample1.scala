/**
 * Copyright 2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"). You may not
 * use this file except in compliance with the License. A copy of the License
 * is located at
 *
 *     http://aws.amazon.com/apache2.0/
 *
 * or in the "license" file accompanying this file. This file is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 *
 */

package com.amazon.deequ.examples

import com.amazon.deequ.VerificationSuite
import com.amazon.deequ.checks.{Check, CheckLevel, CheckStatus}
import com.amazon.deequ.constraints.ConstraintStatus
import com.amazon.deequ.examples.ExampleUtils.{itemsAsDataframe, withSpark}
import java.util.Date

import scala.util.matching.Regex
import scala.tools.scalap.scalax.rules.Rule
object Example{
    val UNIQUE="UNIQUE"
    val NOT_NULLS="NOT_NULLS"
    val TABLE_SIZE = "TABLE_SIZE"
    val MAX_LENGTH = "MAX_LENGTH"
    val MIN_LENGTH = "MIN_LENGTH"
    val MIN_VALUE = "MIN_VALUE"
    val MAX_VALUE = "MAX_VALUE"
    val MEAN_VALUE = "MEAN_VALUE"
    val SUM_VALUE = "SUM_VALUE"
    val STANDARD_DEVIATION= "STANDARD_DEVIATION"
    val REGULAR_EXPRESSION="REGULAR_EXPRESSION"
    val COMPARE_WITH_COLUMN="COMPARE_WITH_COLUMN"
}
case class InputObj(ruleType:String,column:Option[String]=None ,ruleValue:Option[Double]=None, regularExpression:Option[String]=None,operator:Option[String]=None, expectedPercent:Int=100, name : Option[String]=None, creationTime:Option[Date]=None, level:String="Warning")

private[examples] object BasicExample1  {

  def getLevel(level:String) = if(level == "Warning") CheckLevel.Warning else CheckLevel.Error

  def raw(operator: String, v2: Double)=(v1: Double) => {
    operator match {
      case ">" => (v1 > v2)
      case ">=" => (v1 >= v2)
      case "<" => (v1 < v2)
      case "<=" => (v1 <= v2)
      case "=" => (v1 == v2)
      case _ => throw new Exception("Operator not supported")
    }
  }

  def rawSize(operator: String, v2: Long)=(v1: Double) => {
    operator match {
      case ">" => (v1 > v2)
      case ">=" => (v1 >= v2)
      case "<" => (v1 < v2)
      case "<=" => (v1 <= v2)
      case "=" => (v1 == v2)
      case _ => throw new Exception("Operator not supported")
    }
  }

  def getRules(inputRules:List[InputObj]):List[Check]={
    inputRules.map(rule=>{
      rule.ruleType match {
        case Example.UNIQUE => Check(getLevel(rule.level),rule.name.getOrElse(rule.ruleType)).hasUniqueness(rule.column.get, (fraction: Double) => raw(rule.operator.get, fraction)(rule.expectedPercent/100))
        case Example.NOT_NULLS => Check(getLevel(rule.level),rule.name.getOrElse(rule.ruleType)).hasCompleteness(rule.column.get, (fraction: Double) => raw(rule.operator.get, fraction)(rule.expectedPercent/100))
        case Example.MAX_LENGTH => Check(getLevel(rule.level),rule.name.getOrElse(rule.ruleType)).hasMaxLength(rule.column.get, (fraction: Double) => raw(rule.operator.get, fraction)(rule.ruleValue.get/100))
        case Example.MIN_LENGTH => Check(getLevel(rule.level),rule.name.getOrElse(rule.ruleType)).hasMinLength(rule.column.get, (fraction: Double) => raw(rule.operator.get, fraction)(rule.ruleValue.get/100))
        case Example.MEAN_VALUE => Check(getLevel(rule.level),rule.name.getOrElse(rule.ruleType)).hasMean(rule.column.get, (fraction: Double) => raw(rule.operator.get, fraction)(rule.ruleValue.get/100))
        case Example.SUM_VALUE => Check(getLevel(rule.level),rule.name.getOrElse(rule.ruleType)).hasSum(rule.column.get, (fraction: Double) => raw(rule.operator.get, fraction)(rule.ruleValue.get/100))
        case Example.STANDARD_DEVIATION => Check(getLevel(rule.level),rule.name.getOrElse(rule.ruleType)).hasStandardDeviation(rule.column.get, (fraction: Double) => raw(rule.operator.get, fraction)(rule.ruleValue.get/100))
        case Example.REGULAR_EXPRESSION => Check(getLevel(rule.level),rule.name.getOrElse(rule.ruleType)).hasPattern(rule.column.get,rule.regularExpression.get.r)
        case Example.TABLE_SIZE => Check(getLevel(rule.level),rule.name.getOrElse(rule.ruleType)).hasSize( (fraction: Long) => rawSize(rule.operator.get, fraction)(rule.ruleValue.get/100))
      }
    })
  }

  def main(args: Array[String]): Unit = {
    withSpark { session =>
      val data = itemsAsDataframe(session,
        Item(1, "Thingy A", "awesome thing.", "high", 0),
        Item(2, "Thingy B", "available at http://thingb.com", null, 0),
        Item(3, null, null, "low", 5),
        Item(4, "Thingy D", "checkout https://thingd.ca", "low", 10),
        Item(5, "Thingy E", null, "high", 12))

      val verificationResult = VerificationSuite()
        .onData(data)
        .addCheck(
          Check(CheckLevel.Error, "integrity checks")
            // we expect 5 records
            .hasSize(_ == 5)
            // 'id' should never be NULL
            .isComplete("id")
            // 'id' should not contain duplicates
            .isUnique("id")
            // 'productName' should never be NULL
            .isComplete("productName")
            // 'priority' should only contain the values "high" and "low"
            .isContainedIn("priority", Array("high", "low"))
            // 'numViews' should not contain negative values
            .isNonNegative("numViews"))
        .addCheck(
          Check(CheckLevel.Warning, "distribution checks")
            // at least half of the 'description's should contain a url
            .containsURL("description", _ >= 0.5)
            // half of the items should have less than 10 'numViews'
            .hasApproxQuantile("numViews", 0.5, _ <= 10)).addChecks(

        getRules(List(InputObj(ruleType = Example.UNIQUE,column = Some("id"),None,None,operator = Some("="),
          expectedPercent = 100, name=Some("myTest 1")),
          InputObj(ruleType = Example.UNIQUE,column = Some("id"),None,None,operator = Some("="),
            expectedPercent = 100, name=Some("myTest 1")),
          InputObj(ruleType = Example.NOT_NULLS,column = Some("id"),None,None,operator = Some("="),
            expectedPercent = 100, name=Some("not nulls id")),
          InputObj(ruleType = Example.MAX_LENGTH,column = Some("productName"),ruleValue=Some(5),None,operator = Some("="),
            expectedPercent = 100,name=Some("Max length 1")),
          InputObj(ruleType = Example.MIN_LENGTH,column = Some("productName"),ruleValue=Some(20),None,operator = Some("="),
            expectedPercent = 100,name=Some("Min Length 1")),
          InputObj(ruleType = Example.MEAN_VALUE,column = Some("id"),ruleValue=Some(2.5),None,operator = Some("="),
            expectedPercent = 100,name=Some("Mean Value Length 1"))

        ))
      ).addCheck(

        getRules(List(InputObj(ruleType = Example.UNIQUE,column = Some("id"),None,None,operator = Some("<"),
          expectedPercent = 100, name=Some("myTest 2")))).head
      ).addCheck(

        getRules(List(InputObj(ruleType = Example.UNIQUE,column = Some("id"),None,None,operator = Some("="),
          expectedPercent = 50, name=Some("myTest 3")))).head
      )
        .run()
      if (verificationResult.status == CheckStatus.Success) {
        println("The data passed the test, everything is fine!")
      } else {
        println("We found errors in the data, the following constraints were not satisfied:\n")

        val resultsForAllConstraints = verificationResult.checkResults
          .flatMap { case (_, checkResult) => checkResult.constraintResults }

        resultsForAllConstraints
          .filter { _.status != ConstraintStatus.Success }
          .foreach { result =>
            println(s"${result.constraint} failed: ${result.message.get}")
          }
      }
    }
  }


//
//  withSpark { session =>
//
//    val data = itemsAsDataframe(session,
//      Item(1, "Thingy A", "awesome thing.", "high", 0),
//      Item(2, "Thingy B", "available at http://thingb.com", null, 0),
//      Item(3, null, null, "low", 5),
//      Item(4, "Thingy D", "checkout https://thingd.ca", "low", 10),
//      Item(5, "Thingy E", null, "high", 12))
//
//    val verificationResult = VerificationSuite()
//      .onData(data)
//      .addCheck(
//        Check(CheckLevel.Error, "integrity checks")
//          // we expect 5 records
//          .hasSize(_ == 5)
//          // 'id' should never be NULL
//          .isComplete("id")
//          // 'id' should not contain duplicates
//          .isUnique("id")
//          // 'productName' should never be NULL
//          .isComplete("productName")
//          // 'priority' should only contain the values "high" and "low"
//          .isContainedIn("priority", Array("high", "low"))
//          // 'numViews' should not contain negative values
//          .isNonNegative("numViews"))
//      .addCheck(
//        Check(CheckLevel.Warning, "distribution checks")
//          // at least half of the 'description's should contain a url
//          .containsURL("description", _ >= 0.5)
//          // half of the items should have less than 10 'numViews'
//          .hasApproxQuantile("numViews", 0.5, _ <= 10))
//      .run()
//
//    if (verificationResult.status == CheckStatus.Success) {
//      println("The data passed the test, everything is fine!")
//    } else {
//      println("We found errors in the data, the following constraints were not satisfied:\n")
//
//      val resultsForAllConstraints = verificationResult.checkResults
//        .flatMap { case (_, checkResult) => checkResult.constraintResults }
//
//      resultsForAllConstraints
//        .filter { _.status != ConstraintStatus.Success }
//        .foreach { result =>
//          println(s"${result.constraint} failed: ${result.message.get}")
//        }
//    }
//
//  }
}
