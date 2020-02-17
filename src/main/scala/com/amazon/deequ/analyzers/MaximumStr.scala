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

package com.amazon.deequ.analyzers

import com.amazon.deequ.analyzers.Preconditions.{hasColumn, isNumeric}
import org.apache.spark.sql.{Column, Row}
import org.apache.spark.sql.functions.max
import org.apache.spark.sql.types.{DoubleType, StructType}
import Analyzers._

case class MaxStrState(maxValue: String) extends StringValuedState[MaxStrState] {

  override def sum(other: MaxStrState): MaxStrState = {
    MaxStrState(if(maxValue > other.maxValue) maxValue else other.maxValue)
  }

  override def metricValue(): String = {
    maxValue
  }
}

case class MaximumStr(column: String, where: Option[String] = None)
  extends StandardStringScanShareableAnalyzer[MaxStrState]("MaximumStr", column) {

  override def aggregationFunctions(): Seq[Column] = {
    max(conditionalSelection(column, where)) :: Nil
  }

  override def fromAggregationResult(result: Row, offset: Int): Option[MaxStrState] = {

    ifNoNullsIn(result, offset) { _ =>
      MaxStrState(result.getString(offset))
    }
  }

  override protected def additionalPreconditions(): Seq[StructType => Unit] = {
    hasColumn(column)  :: Nil
  }
}
