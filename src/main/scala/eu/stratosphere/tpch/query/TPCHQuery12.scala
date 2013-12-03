/* *********************************************************************************************************************
 *
 * Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 * ********************************************************************************************************************
 */
package eu.stratosphere.tpch.query

import scala.language.reflectiveCalls

import eu.stratosphere.scala._
import eu.stratosphere.scala.operators._

import eu.stratosphere.tpch.schema._
import org.joda.time.DateTime

/**
 * Original query:
 *
 * {{{
 * 
 * select
 * l_shipmode, 
 * sum(case 
 * when o_orderpriority ='1-URGENT'
 * or o_orderpriority ='2-HIGH'
 * then 1
 * else 0
 * end) as high_line_count,
 * sum(case 
 * when o_orderpriority <> '1-URGENT'
 * and o_orderpriority <> '2-HIGH'
 * then 1
 * else 0
 * end) as low_line_count
 * from 
 * orders, 
 * lineitem
 * where 
 * o_orderkey = l_orderkey
 * and l_shipmode in ('[SHIPMODE1]', '[SHIPMODE2]')
 * and l_commitdate < l_receiptdate
 * and l_shipdate < l_commitdate
 * and l_receiptdate >= date '[DATE]'
 * and l_receiptdate < date '[DATE]' + interval '1' year
 * group by 
 * l_shipmode
 * order by 
 * l_shipmode;
 * }}}
 *
 * @param dop Degree of parallism
 * @param inPath Base input path
 * @param outPath Output path
 * @param shipmodes Query parameters `SHIPMODE1` and `SHIPMODE2` -> default: MAIl, SHIP
 * @param date Query parameter `DATE` -> default: 1994-01-01
 */
class TPCHQuery12(dop: Int, inPath: String, outPath: String, shipmodes: List[String], date: DateTime) extends TPCHQuery(12, dop, inPath, outPath) {

  def plan(): ScalaPlan = {

    
    val lineitem = Lineitem(inPath) filter { x => TPCHQuery.string2date(x.shipDate).compareTo(TPCHQuery.string2date(x.commitDate)) < 0 && TPCHQuery.string2date(x.commitDate).compareTo(TPCHQuery.string2date(x.receiptDate)) < 0 &&  TPCHQuery.string2date(x.receiptDate).compareTo(date.plusYears(1)) == -1  && TPCHQuery.string2date(x.receiptDate).compareTo(date) > -1 &&  TPCHQuery.string2date(x.receiptDate).compareTo(date.plusYears(1)) == -1 && shipmodes.contains(x.shipMode) }
    val order = Order(inPath)
    

    val e1 = lineitem join order where (_.orderKey) isEqualTo (_.orderKey) map {
      (l, o) => (l.shipMode, if(o.orderPriority == "1-URGENT" || o.orderPriority == "2-HIGH") 1 else 0, if(o.orderPriority != "1-URGENT" && o.orderPriority != "2-HIGH") 1 else 0 )
    } groupBy(_._1) reduce {
      (x, y) => (x._1, x._2 + y._2,  x._3 + y._3)
    }
    
    
    // TODO: sort e4 on (_1 )

    val expression = e1.write(s"$outPath/query12.result", DelimitedOutputFormat(x => "%s|%d|%d".format(x._1, x._2, x._3)))

    val plan = new ScalaPlan(Seq(expression), queryName)
    plan.setDefaultParallelism(dop)

    plan
  }
}
