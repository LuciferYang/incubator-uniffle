/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.uniffle.client.request;

import org.junit.jupiter.api.Test;

import org.apache.uniffle.proto.RssProtos;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RssUnregisterShuffleRequestTest {

  @Test
  void toProtoShouldPreserveExplicitStageAttemptZero() {
    RssUnregisterShuffleRequest request =
        new RssUnregisterShuffleRequest("app-1", 2, 3, Integer.valueOf(0));

    RssProtos.ShuffleUnregisterRequest proto = request.toProto();

    assertTrue(request.hasStageAttemptNumber());
    assertEquals(0, request.getStageAttemptNumber());
    assertTrue(proto.hasStageAttemptNumber());
    assertEquals(0, proto.getStageAttemptNumber());
  }

  @Test
  void toProtoShouldKeepOldOverloadStageAttemptAbsent() {
    RssUnregisterShuffleRequest request = new RssUnregisterShuffleRequest("app-1", 2, 3);

    RssProtos.ShuffleUnregisterRequest proto = request.toProto();

    assertFalse(request.hasStageAttemptNumber());
    assertEquals(0, request.getStageAttemptNumber());
    assertFalse(proto.hasStageAttemptNumber());
  }

  @Test
  void finishShuffleRequestShouldPreserveOptionalStageAttempt() {
    RssFinishShuffleRequest legacyRequest = new RssFinishShuffleRequest("app-1", 2);
    RssFinishShuffleRequest stageRequest =
        new RssFinishShuffleRequest("app-1", 2, Integer.valueOf(0));

    assertFalse(legacyRequest.hasStageAttemptNumber());
    assertEquals(0, legacyRequest.getStageAttemptNumber());
    assertTrue(stageRequest.hasStageAttemptNumber());
    assertEquals(0, stageRequest.getStageAttemptNumber());
  }

  @Test
  void startSortMergeRequestShouldPreserveOptionalStageAttempt() {
    RssStartSortMergeRequest legacyRequest =
        new RssStartSortMergeRequest("app-1", 2, 3, null);
    RssStartSortMergeRequest stageRequest =
        new RssStartSortMergeRequest("app-1", 2, 3, null, Integer.valueOf(0));

    assertFalse(legacyRequest.hasStageAttemptNumber());
    assertEquals(0, legacyRequest.getStageAttemptNumber());
    assertTrue(stageRequest.hasStageAttemptNumber());
    assertEquals(0, stageRequest.getStageAttemptNumber());
  }
}
