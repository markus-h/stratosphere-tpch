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

/**
 * Original query:
 *
 * {{{
select
	nation,
	o_year,
	sum(amount) as sum_profit
from
	(
		select
			n_name as nation,
			extract(year from o_orderdate) as o_year,
			l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity as amount
		from
			part,
			supplier,
			lineitem,
			partsupp,
			orders,
			nation
		where
			s_suppkey = l_suppkey *
			and ps_suppkey = l_suppkey *
			and ps_partkey = l_partkey *
			and p_partkey = l_partkey *
			and o_orderkey = l_orderkey
			and s_nationkey = n_nationkey *
			and p_name like '%:1%' *
	) as profit
group by
	nation,
	o_year
order by
	nation,
	o_year desc;
 * }}}
 *
 * @param dop Degree of parallism
 * @param inPath Base input path
 * @param outPath Output path
 * @param pname Query parameter `NAME`
 */
class TPCHQuery0Q(dop: Int, inPath: String, outPath: String, pname: String) extends TPCHQuery(2, dop, inPath, outPath) {

  def plan(): ScalaPlan = {
  
  
    val part = Part(inPath) filter (p => p.name.indexOf(this.pname) != -1)
    val supplier = Supplier(inPath)
    val lineitem = Lineitem(inPath)
    val partsupp = PartSupp(inPath)
    val order = Order(inPath)
    val nation = Nation(inPath)

    var e1 = lineitem join part where (_.partkey) isEqualTo (_.partkey) map {
      (l, p) => (l.suppkey, l.partkey, l.orderkey, l.extendedprice, l.discount, l.quantity)
    }
	
    val e2 = e1 join supplier where (_._1) isEqualTo (_.suppkey) map {
      (x, s) => (x._1, x._2, x._3, x._4, x._5, x._6, s.nationkey)
    }
	
    val e3 = e2 join nation where (_._7) isEqualTo (_.nationkey) map {
      (x, n) => (x._1, x._2, x._3, x._4, x._5, x._6, n.name)
    }
	
    val e4 = e3 join partsupp where (_._2) isEqualTo (_.partkey) AND (_._1) isEqualTo (_.suppkey) map {
	  (x, ps) => (x._3, x._4 * (1 -  x._5) - ps.supplycost * x._6, x._7)
    }
	
    val e5 = e4 join order where (_._1) isEqualTo (_.orderkey) map {
      (x, ps) => (x._2, x._3, TPCHQuery.string2date(o.orderdate).EXTRACT_YEAR)
    }
	
    val e6 = e5.groupBy(x => (x._2, x._3)).reduce(
      (x, y) => (x._2, x._3, x._1 + y._1)
    )

    // TODO: sort e6 on (_1 asc, _2 desc)

    val expression = e4.write(s"$outPath/query02.result", DelimitedOutputFormat(x => "%s|%s|%f".format(x._1, x._2, x._3)))

    val plan = new ScalaPlan(Seq(expression), queryName)
    plan.setDefaultParallelism(dop)

    plan
  }
}
}
