/**
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

package kafka.utils

import org.apache.kafka.clients.{ClientRequest, ClientResponse, NetworkClient, NetworkClientUtils}
import org.apache.kafka.common.Node
import org.apache.kafka.common.utils.Time

object NetworkClientBlockingOps {
  implicit def networkClientBlockingOps(client: NetworkClient): NetworkClientBlockingOps =
    new NetworkClientBlockingOps(client)
}

/**
 * Provides extension methods for `NetworkClient` that are useful for implementing blocking behaviour. Use with care.
 *
 * Example usage:
 *
 * {{{
 * val networkClient: NetworkClient = ...
 * import NetworkClientBlockingOps._
 * networkClient.blockingReady(...)
 * }}}
 */
class NetworkClientBlockingOps(val client: NetworkClient) extends AnyVal {

  /**
   * See [[NetworkClientUtils.isReady()]]
   */
  def isReady(node: Node)(implicit time: Time): Boolean =
    NetworkClientUtils.isReady(client, node, time.milliseconds())

  /**
   * See [[NetworkClientUtils.awaitReady()]]
   */
  def blockingReady(node: Node, timeout: Long)(implicit time: Time): Boolean =
    NetworkClientUtils.awaitReady(client, node, time, timeout)

  /**
   * See [[NetworkClientUtils.sendAndReceive()]]
   */
  def blockingSendAndReceive(request: ClientRequest)(implicit time: Time): ClientResponse =
    NetworkClientUtils.sendAndReceive(client, request, time)

}
