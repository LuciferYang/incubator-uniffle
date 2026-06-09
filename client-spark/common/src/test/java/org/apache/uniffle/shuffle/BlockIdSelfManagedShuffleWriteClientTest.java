/*
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

package org.apache.uniffle.shuffle;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.roaringbitmap.longlong.Roaring64NavigableMap;

import org.apache.uniffle.client.PartitionDataReplicaRequirementTracking;
import org.apache.uniffle.client.api.ShuffleManagerClient;
import org.apache.uniffle.client.api.ShuffleResult;
import org.apache.uniffle.client.api.ShuffleWriteClient;
import org.apache.uniffle.client.request.RssGetShuffleResultForMultiPartRequest;
import org.apache.uniffle.client.request.RssReportShuffleResultRequest;
import org.apache.uniffle.client.response.RssGetShuffleResultResponse;
import org.apache.uniffle.client.response.RssReportShuffleResultResponse;
import org.apache.uniffle.common.ClientType;
import org.apache.uniffle.common.ShuffleServerInfo;
import org.apache.uniffle.common.exception.RssException;
import org.apache.uniffle.common.rpc.StatusCode;
import org.apache.uniffle.common.util.RssUtils;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class BlockIdSelfManagedShuffleWriteClientTest {

  @Test
  public void reportShuffleResultShouldForwardStageAttemptNumberToManager() {
    ShuffleManagerClient managerClient = mock(ShuffleManagerClient.class);
    BlockIdSelfManagedShuffleWriteClient client = createClient(managerClient);
    Map<ShuffleServerInfo, Map<Integer, Set<Long>>> blockIds = createBlockIds();

    client.reportShuffleResult(
        blockIds, "app-1", 2, 3L, 1, 4, Collections.emptySet(), true);

    ArgumentCaptor<RssReportShuffleResultRequest> captor =
        ArgumentCaptor.forClass(RssReportShuffleResultRequest.class);
    verify(managerClient).reportShuffleResult(captor.capture());
    RssReportShuffleResultRequest request = captor.getValue();

    assertEquals(4, request.getStageAttemptNumber());
    assertEquals(4, request.toProto().getStageAttemptNumber());
    assertTrue(request.toProto().hasStageAttemptNumber());
    assertEquals(Collections.singletonList(10L), request.getPartitionToBlockIds().get(0));
  }

  @Test
  public void reportShuffleResultOldOverloadShouldDefaultStageAttemptNumberToZero() {
    ShuffleManagerClient managerClient = mock(ShuffleManagerClient.class);
    BlockIdSelfManagedShuffleWriteClient client = createClient(managerClient);

    client.reportShuffleResult(createBlockIds(), "app-1", 2, 3L, 1);

    ArgumentCaptor<RssReportShuffleResultRequest> captor =
        ArgumentCaptor.forClass(RssReportShuffleResultRequest.class);
    verify(managerClient, times(1)).reportShuffleResult(captor.capture());
    RssReportShuffleResultRequest request = captor.getValue();

    assertEquals(0, request.getStageAttemptNumber());
    assertFalse(request.toProto().hasStageAttemptNumber());
  }

  @Test
  public void reportShuffleResultNewOverloadShouldSetStageAttemptNumberWhenZero() {
    ShuffleManagerClient managerClient = mock(ShuffleManagerClient.class);
    BlockIdSelfManagedShuffleWriteClient client = createClient(managerClient);

    client.reportShuffleResult(createBlockIds(), "app-1", 2, 3L, 1, 0);

    ArgumentCaptor<RssReportShuffleResultRequest> captor =
        ArgumentCaptor.forClass(RssReportShuffleResultRequest.class);
    verify(managerClient, times(1)).reportShuffleResult(captor.capture());
    RssReportShuffleResultRequest request = captor.getValue();

    assertEquals(0, request.getStageAttemptNumber());
    assertTrue(request.toProto().hasStageAttemptNumber());
  }

  @Test
  public void getShuffleResultForMultiPartV2ShouldReadFromManager() throws Exception {
    ShuffleManagerClient managerClient = mock(ShuffleManagerClient.class);
    Roaring64NavigableMap expected = Roaring64NavigableMap.bitmapOf(1L, 2L);
    when(managerClient.getShuffleResultForMultiPart(any()))
        .thenReturn(
            new RssGetShuffleResultResponse(
                StatusCode.SUCCESS, RssUtils.serializeBitMap(expected)));
    BlockIdSelfManagedShuffleWriteClient client = createClient(managerClient);

    ShuffleResult result =
        client.getShuffleResultForMultiPartV2(
            ClientType.GRPC.name(),
            Collections.singletonMap(
                new ShuffleServerInfo("server-1", "127.0.0.1", 19999),
                Collections.singleton(0)),
            "app-1",
            2,
            Collections.emptySet(),
            mock(PartitionDataReplicaRequirementTracking.class));

    assertEquals(2, result.getBlockIds().getLongCardinality());
    assertTrue(result.getBlockIds().contains(1L));
    assertTrue(result.getBlockIds().contains(2L));
    ArgumentCaptor<RssGetShuffleResultForMultiPartRequest> captor =
        ArgumentCaptor.forClass(RssGetShuffleResultForMultiPartRequest.class);
    verify(managerClient, times(1)).getShuffleResultForMultiPart(captor.capture());
    assertEquals("app-1", captor.getValue().getAppId());
    assertEquals(2, captor.getValue().getShuffleId());
    assertEquals(Collections.singleton(0), captor.getValue().getPartitions());
  }

  @Test
  public void reportShuffleResultNewRecordNumberOverloadShouldSetStageAttemptNumber() {
    ShuffleManagerClient managerClient = mock(ShuffleManagerClient.class);
    BlockIdSelfManagedShuffleWriteClient client = createClient(managerClient);
    Map<ShuffleServerInfo, Map<Integer, Set<Long>>> blockIds = createBlockIds();

    client.reportShuffleResult(
        blockIds, "app-1", 2, 3L, 1, 7, Collections.emptySet(), true, createRecordNumbers(blockIds));

    ArgumentCaptor<RssReportShuffleResultRequest> captor =
        ArgumentCaptor.forClass(RssReportShuffleResultRequest.class);
    verify(managerClient, times(1)).reportShuffleResult(captor.capture());
    RssReportShuffleResultRequest request = captor.getValue();

    assertEquals(7, request.getStageAttemptNumber());
    assertTrue(request.toProto().hasStageAttemptNumber());
    assertEquals(1, request.toProto().getPartitionStatsCount());
    assertEquals(0, request.toProto().getPartitionStats(0).getPartitionId());
    assertEquals(
        1L,
        request.toProto().getPartitionStats(0).getTaskAttemptIdToRecords(0).getRecordNumber());
  }

  @Test
  public void reportShuffleResultOldRecordNumberOverloadShouldNotSetStageAttemptNumber() {
    ShuffleManagerClient managerClient = mock(ShuffleManagerClient.class);
    BlockIdSelfManagedShuffleWriteClient client = createClient(managerClient);
    Map<ShuffleServerInfo, Map<Integer, Set<Long>>> blockIds = createBlockIds();

    client.reportShuffleResult(
        blockIds, "app-1", 2, 3L, 1, Collections.emptySet(), true, createRecordNumbers(blockIds));

    ArgumentCaptor<RssReportShuffleResultRequest> captor =
        ArgumentCaptor.forClass(RssReportShuffleResultRequest.class);
    verify(managerClient, times(1)).reportShuffleResult(captor.capture());
    RssReportShuffleResultRequest request = captor.getValue();

    assertEquals(0, request.getStageAttemptNumber());
    assertFalse(request.toProto().hasStageAttemptNumber());
    assertEquals(1, request.toProto().getPartitionStatsCount());
  }

  @Test
  public void reportShuffleResultShouldNotDoubleCountReplicatedRecordNumbers() {
    ShuffleManagerClient managerClient = mock(ShuffleManagerClient.class);
    BlockIdSelfManagedShuffleWriteClient client = createClient(managerClient);
    Map<ShuffleServerInfo, Map<Integer, Set<Long>>> blockIds = createBlockIds();
    Map<ShuffleServerInfo, Map<Integer, Long>> recordNumbers = new HashMap<>();
    recordNumbers.put(
        new ShuffleServerInfo("server-1", "127.0.0.1", 19999), Collections.singletonMap(0, 5L));
    recordNumbers.put(
        new ShuffleServerInfo("server-2", "127.0.0.2", 19999), Collections.singletonMap(0, 5L));

    client.reportShuffleResult(
        blockIds, "app-1", 2, 3L, 1, 0, Collections.emptySet(), true, recordNumbers);

    ArgumentCaptor<RssReportShuffleResultRequest> captor =
        ArgumentCaptor.forClass(RssReportShuffleResultRequest.class);
    verify(managerClient, times(1)).reportShuffleResult(captor.capture());
    RssReportShuffleResultRequest request = captor.getValue();

    assertTrue(request.toProto().hasStageAttemptNumber());
    assertEquals(1, request.toProto().getPartitionStatsCount());
    assertEquals(
        5L,
        request.toProto().getPartitionStats(0).getTaskAttemptIdToRecords(0).getRecordNumber());
  }

  @Test
  public void reportShuffleResultShouldRejectStaleStageAttemptFromManager() {
    ShuffleManagerClient managerClient = mock(ShuffleManagerClient.class);
    BlockIdSelfManagedShuffleWriteClient client = createClient(managerClient);
    when(managerClient.reportShuffleResult(any()))
        .thenReturn(
            new RssReportShuffleResultResponse(StatusCode.STAGE_RETRY_IGNORE, false, true));

    RssException exception =
        assertThrows(
            RssException.class,
            () ->
                client.reportShuffleResult(
                    createBlockIds(),
                    "app-1",
                    2,
                    3L,
                    1,
                    0,
                    Collections.emptySet(),
                    true));

    assertTrue(exception.getMessage().contains("STAGE_RETRY_IGNORE"));
  }

  @Test
  public void reportShuffleResultShouldRejectStageAttemptAckFalseFromManager() {
    ShuffleManagerClient managerClient = mock(ShuffleManagerClient.class);
    BlockIdSelfManagedShuffleWriteClient client = createClient(managerClient);
    when(managerClient.reportShuffleResult(any()))
        .thenReturn(new RssReportShuffleResultResponse(StatusCode.SUCCESS, false, true));

    RssException exception =
        assertThrows(
            RssException.class,
            () ->
                client.reportShuffleResult(
                    createBlockIds(),
                    "app-1",
                    2,
                    3L,
                    1,
                    0,
                    Collections.emptySet(),
                    true));

    assertTrue(exception.getMessage().contains("stageAttemptNumber"));
  }

  @Test
  public void reportShuffleResultShouldRejectMissingStageAttemptAckFromManager() {
    ShuffleManagerClient managerClient = mock(ShuffleManagerClient.class);
    BlockIdSelfManagedShuffleWriteClient client = createClient(managerClient);
    when(managerClient.reportShuffleResult(any()))
        .thenReturn(new RssReportShuffleResultResponse(StatusCode.SUCCESS));

    RssException exception =
        assertThrows(
            RssException.class,
            () ->
                client.reportShuffleResult(
                    createBlockIds(),
                    "app-1",
                    2,
                    3L,
                    1,
                    0,
                    Collections.emptySet(),
                    true));

    assertTrue(exception.getMessage().contains("stageAttemptNumber"));
  }

  @Test
  public void stageAwareDefaultOverloadShouldDelegateToOldOverload() {
    ShuffleWriteClient client = mock(ShuffleWriteClient.class, Mockito.CALLS_REAL_METHODS);
    Map<ShuffleServerInfo, Map<Integer, Set<Long>>> blockIds = createBlockIds();

    client.reportShuffleResult(blockIds, "app-1", 2, 3L, 1, 0);
    verify(client).reportShuffleResult(blockIds, "app-1", 2, 3L, 1);

    UnsupportedOperationException recordNumberException =
        assertThrows(
            UnsupportedOperationException.class,
            () ->
                client.reportShuffleResult(
                    blockIds,
                    "app-1",
                    2,
                    3L,
                    1,
                    0,
                    Collections.emptySet(),
                    true,
                    createRecordNumbers(blockIds)));

    assertTrue(
        recordNumberException
            .getMessage()
            .contains("reportShuffleResult with integrity validation mechanism"));
  }

  private BlockIdSelfManagedShuffleWriteClient createClient(ShuffleManagerClient managerClient) {
    when(managerClient.reportShuffleResult(any()))
        .thenReturn(new RssReportShuffleResultResponse(StatusCode.SUCCESS, true, true));
    return (BlockIdSelfManagedShuffleWriteClient)
        RssShuffleClientFactory.newWriteBuilder()
            .clientType(ClientType.GRPC.name())
            .retryMax(3)
            .retryIntervalMax(2000)
            .heartBeatThreadNum(4)
            .replica(1)
            .replicaWrite(1)
            .replicaRead(1)
            .replicaSkipEnabled(true)
            .dataTransferPoolSize(1)
            .dataCommitPoolSize(1)
            .unregisterThreadPoolSize(10)
            .unregisterTimeSec(10)
            .unregisterRequestTimeSec(10)
            .managerClientSupplier(() -> managerClient)
            .blockIdSelfManagedEnabled(true)
            .build();
  }

  private Map<ShuffleServerInfo, Map<Integer, Set<Long>>> createBlockIds() {
    Map<Integer, Set<Long>> partitionToBlockIds = new HashMap<>();
    partitionToBlockIds.put(0, Collections.singleton(10L));
    Map<ShuffleServerInfo, Map<Integer, Set<Long>>> serverToPartitionToBlockIds = new HashMap<>();
    serverToPartitionToBlockIds.put(
        new ShuffleServerInfo("server-1", "127.0.0.1", 19999), partitionToBlockIds);
    return serverToPartitionToBlockIds;
  }

  private Map<ShuffleServerInfo, Map<Integer, Long>> createRecordNumbers(
      Map<ShuffleServerInfo, Map<Integer, Set<Long>>> blockIds) {
    Map<ShuffleServerInfo, Map<Integer, Long>> recordNumbers = new HashMap<>();
    for (Map.Entry<ShuffleServerInfo, Map<Integer, Set<Long>>> serverEntry : blockIds.entrySet()) {
      Map<Integer, Long> partitionToRecordNumbers = new HashMap<>();
      for (Map.Entry<Integer, Set<Long>> partitionEntry : serverEntry.getValue().entrySet()) {
        partitionToRecordNumbers.put(
            partitionEntry.getKey(), (long) partitionEntry.getValue().size());
      }
      recordNumbers.put(serverEntry.getKey(), partitionToRecordNumbers);
    }
    return recordNumbers;
  }
}
