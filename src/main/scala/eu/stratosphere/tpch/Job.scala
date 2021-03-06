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
package eu.stratosphere.tpch

import eu.stratosphere.pact.client.LocalExecutor
import eu.stratosphere.pact.client.RemoteExecutor
import eu.stratosphere.tpch.config.TPCHConfig
import eu.stratosphere.tpch.query.TPCHQuery
import eu.stratosphere.scala.ScalaPlan
import eu.stratosphere.tpch.config.TPCHConfig

import scopt.TPCHOptionParser

/**
 * Abstract job runner encapsulating common driver logic.
 */
abstract class AbstractJobRunner {

  /**
   * Main method.
   */
  def main(args: Array[String]) {

    TPCHOptionParser().parse(args, TPCHConfig()) map { config =>
      try {
        TPCHQuery.createQuery(config)
          .map(query => executeQuery(query.plan()))
          .getOrElse {
            System.err.println(f"Sorry, TPC-H Query #${config.queryNo}%02d is not yet supported.")
          }
      } catch {
        case e: Throwable => {
          System.err.println("Unexpected error during execution: " + e.getMessage())
          e.printStackTrace(System.err)
          System.exit(-1)
        }
      }
    } getOrElse {
      System.exit(-1)
    }
  }

  /**
   * Executes the query in a specific environment (local or remote).
   */
  def executeQuery(plan: ScalaPlan)
}

/**
 * To run TPCH Query X locally with this class using:
 * mvn exec:exec -Dexec.executable="java" -Dexec.args="-cp %classpath eu.stratosphere.tpch.LocalJobRunner QXX 2 file:///tpch/path file:///output/path <Query-X-args>"
 */
object LocalJobRunner extends AbstractJobRunner {

  def executeQuery(plan: ScalaPlan) {
    LocalExecutor.execute(plan)
  }
}

/**
 * To run TPCH Query X on a cluster with this class using:
 * mvn exec:exec -Dexec.executable="java" -Dexec.args="-cp %classpath eu.stratosphere.tpch.RemoteJobRunner QXX 2 file:///input/path file:///output/path <Query-X-args>"
 */
object RemoteJobRunner extends AbstractJobRunner {

  def executeQuery(plan: ScalaPlan) {
    (new RemoteExecutor("localhost", 6123, "target/stratosphere-tpch-bin.jar")).executePlan(plan)
  }
}
