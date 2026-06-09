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

package org.apache.uniffle.client.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;
import org.roaringbitmap.longlong.Roaring64NavigableMap;

import org.apache.uniffle.client.api.ShuffleServerClient;
import org.apache.uniffle.client.factory.ShuffleClientFactory;
import org.apache.uniffle.client.request.RssFinishShuffleRequest;
import org.apache.uniffle.client.request.RssReportShuffleResultRequest;
import org.apache.uniffle.client.request.RssSendCommitRequest;
import org.apache.uniffle.client.response.RssGetShuffleResultResponse;
import org.apache.uniffle.client.response.RssFinishShuffleResponse;
import org.apache.uniffle.client.response.RssReportShuffleResultResponse;
import org.apache.uniffle.client.response.RssSendCommitResponse;
import org.apache.uniffle.client.response.RssSendShuffleDataResponse;
import org.apache.uniffle.client.response.RssStartSortMergeResponse;
import org.apache.uniffle.client.response.RssUnregisterShuffleResponse;
import org.apache.uniffle.client.response.SendShuffleDataResult;
import org.apache.uniffle.common.ClientType;
import org.apache.uniffle.common.ShuffleBlockInfo;
import org.apache.uniffle.common.ShuffleServerInfo;
import org.apache.uniffle.common.config.RssClientConf;
import org.apache.uniffle.common.config.RssConf;
import org.apache.uniffle.common.exception.RssException;
import org.apache.uniffle.common.exception.RssFetchFailedException;
import org.apache.uniffle.common.exception.RssSendFailedException;
import org.apache.uniffle.common.netty.IOMode;
import org.apache.uniffle.common.rpc.StatusCode;
import org.apache.uniffle.common.util.BlockIdLayout;
import org.apache.uniffle.common.util.RssUtils;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ShuffleWriteClientImplTest {

  @Test
  public void testAbandonEventWhenTaskFailed() {
    ShuffleWriteClientImpl shuffleWriteClient =
        ShuffleClientFactory.newWriteBuilder()
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
            .build();
    ShuffleServerClient mockShuffleServerClient = mock(ShuffleServerClient.class);
    ShuffleWriteClientImpl spyClient = Mockito.spy(shuffleWriteClient);
    doReturn(mockShuffleServerClient).when(spyClient).getShuffleServerClient(any());

    when(mockShuffleServerClient.sendShuffleData(any()))
        .thenAnswer(
            (Answer<String>)
                invocation -> {
                  Thread.sleep(50000);
                  return "ABCD1234";
                });

    List<ShuffleServerInfo> shuffleServerInfoList =
        Lists.newArrayList(new ShuffleServerInfo("id", "host", 0));
    List<ShuffleBlockInfo> shuffleBlockInfoList =
        Lists.newArrayList(
            new ShuffleBlockInfo(
                0, 0, 10, 10, 10, new byte[] {10}, shuffleServerInfoList, 10, 100, 0));

    // It should directly exit and wont do rpc request.
    Awaitility.await()
        .timeout(1, TimeUnit.SECONDS)
        .until(
            () -> {
              spyClient.sendShuffleData("appId", shuffleBlockInfoList, () -> true);
              return true;
            });
  }

  @Test
  public void testSendData() {
    ShuffleWriteClientImpl shuffleWriteClient =
        ShuffleClientFactory.newWriteBuilder()
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
            .build();
    ShuffleServerClient mockShuffleServerClient = mock(ShuffleServerClient.class);
    ShuffleWriteClientImpl spyClient = Mockito.spy(shuffleWriteClient);
    doReturn(mockShuffleServerClient).when(spyClient).getShuffleServerClient(any());
    when(mockShuffleServerClient.sendShuffleData(any()))
        .thenReturn(new RssSendShuffleDataResponse(StatusCode.NO_BUFFER));

    List<ShuffleServerInfo> shuffleServerInfoList =
        Lists.newArrayList(new ShuffleServerInfo("id", "host", 0));
    List<ShuffleBlockInfo> shuffleBlockInfoList =
        Lists.newArrayList(
            new ShuffleBlockInfo(
                0, 0, 10, 10, 10, new byte[] {10}, shuffleServerInfoList, 10, 100, 0));
    SendShuffleDataResult result =
        spyClient.sendShuffleData("appId", shuffleBlockInfoList, () -> false);

    assertTrue(result.getFailedBlockIds().contains(10L));
  }

  @Test
  public void testRegisterAndUnRegisterShuffleServer() {
    ShuffleWriteClientImpl shuffleWriteClient =
        ShuffleClientFactory.newWriteBuilder()
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
            .build();
    String appId1 = "testRegisterAndUnRegisterShuffleServer-1";
    String appId2 = "testRegisterAndUnRegisterShuffleServer-2";
    ShuffleServerInfo server1 = new ShuffleServerInfo("host1-0", "host1", 0);
    ShuffleServerInfo server2 = new ShuffleServerInfo("host2-0", "host2", 0);
    ShuffleServerInfo server3 = new ShuffleServerInfo("host3-0", "host3", 0);
    shuffleWriteClient.addShuffleServer(appId1, 0, server1);
    shuffleWriteClient.addShuffleServer(appId1, 1, server2);
    shuffleWriteClient.addShuffleServer(appId2, 1, server3);
    assertEquals(2, shuffleWriteClient.getAllShuffleServers(appId1).size());
    assertEquals(1, shuffleWriteClient.getAllShuffleServers(appId2).size());
    shuffleWriteClient.addShuffleServer(appId1, 1, server1);
    shuffleWriteClient.unregisterShuffle(appId1, 1);
    assertEquals(1, shuffleWriteClient.getAllShuffleServers(appId1).size());
    shuffleWriteClient.unregisterShuffle(appId1);
    assertEquals(0, shuffleWriteClient.getAllShuffleServers(appId1).size());
    shuffleWriteClient.addShuffleServer(appId2, 2, server1);
    assertEquals(2, shuffleWriteClient.getAllShuffleServers(appId2).size());
    shuffleWriteClient.unregisterShuffle(appId2);
    assertEquals(0, shuffleWriteClient.getAllShuffleServers(appId2).size());
  }

  @Test
  public void stageAwareUnregisterShuffleShouldFailStrictlyAndKeepServerMapping() {
    ShuffleWriteClientImpl shuffleWriteClient =
        ShuffleClientFactory.newWriteBuilder()
            .clientType(ClientType.GRPC.name())
            .retryMax(1)
            .retryIntervalMax(100)
            .heartBeatThreadNum(1)
            .replica(1)
            .replicaWrite(1)
            .replicaRead(1)
            .replicaSkipEnabled(true)
            .dataTransferPoolSize(1)
            .dataCommitPoolSize(1)
            .unregisterThreadPoolSize(1)
            .unregisterTimeSec(1)
            .unregisterRequestTimeSec(1)
            .build();
    String appId = "stageAwareUnregisterShuffleShouldFailStrictly";
    ShuffleServerInfo server = new ShuffleServerInfo("unreachable", "127.0.0.1", 1);
    shuffleWriteClient.addShuffleServer(appId, 1, server);

    assertThrows(RssException.class, () -> shuffleWriteClient.unregisterShuffle(appId, 1, 0));

    assertEquals(1, shuffleWriteClient.getAllShuffleServers(appId).size());
  }

  @Test
  public void stageAwareUnregisterShuffleShouldRejectSuccessWithoutCleanupAck() {
    ShuffleWriteClientImpl shuffleWriteClient =
        ShuffleClientFactory.newWriteBuilder()
            .clientType(ClientType.GRPC.name())
            .retryMax(1)
            .retryIntervalMax(100)
            .heartBeatThreadNum(1)
            .replica(1)
            .replicaWrite(1)
            .replicaRead(1)
            .replicaSkipEnabled(true)
            .dataTransferPoolSize(1)
            .dataCommitPoolSize(1)
            .unregisterThreadPoolSize(1)
            .unregisterTimeSec(1)
            .unregisterRequestTimeSec(1)
            .build();
    ShuffleServerClient mockShuffleServerClient = mock(ShuffleServerClient.class);
    ShuffleWriteClientImpl spyClient = Mockito.spy(shuffleWriteClient);
    doReturn(mockShuffleServerClient).when(spyClient).getShuffleServerClient(any());
    when(mockShuffleServerClient.unregisterShuffle(any()))
        .thenReturn(new RssUnregisterShuffleResponse(StatusCode.SUCCESS));
    String appId = "stageAwareUnregisterShuffleShouldRejectSuccessWithoutCleanupAck";
    ShuffleServerInfo server = new ShuffleServerInfo("server-1", "127.0.0.1", 1);
    spyClient.addShuffleServer(appId, 1, server);

    assertThrows(RssException.class, () -> spyClient.unregisterShuffle(appId, 1, 0));

    assertEquals(1, spyClient.getAllShuffleServers(appId).size());
  }

  @Test
  public void stageAwareUnregisterShuffleShouldRejectCleanupAckFalse() {
    ShuffleWriteClientImpl shuffleWriteClient =
        ShuffleClientFactory.newWriteBuilder()
            .clientType(ClientType.GRPC.name())
            .retryMax(1)
            .retryIntervalMax(100)
            .heartBeatThreadNum(1)
            .replica(1)
            .replicaWrite(1)
            .replicaRead(1)
            .replicaSkipEnabled(true)
            .dataTransferPoolSize(1)
            .dataCommitPoolSize(1)
            .unregisterThreadPoolSize(1)
            .unregisterTimeSec(1)
            .unregisterRequestTimeSec(1)
            .build();
    ShuffleServerClient mockShuffleServerClient = mock(ShuffleServerClient.class);
    ShuffleWriteClientImpl spyClient = Mockito.spy(shuffleWriteClient);
    doReturn(mockShuffleServerClient).when(spyClient).getShuffleServerClient(any());
    when(mockShuffleServerClient.unregisterShuffle(any()))
        .thenReturn(new RssUnregisterShuffleResponse(StatusCode.SUCCESS, false));
    String appId = "stageAwareUnregisterShuffleShouldRejectCleanupAckFalse";
    ShuffleServerInfo server = new ShuffleServerInfo("server-1", "127.0.0.1", 1);
    spyClient.addShuffleServer(appId, 1, server);

    assertThrows(RssException.class, () -> spyClient.unregisterShuffle(appId, 1, 0));

    assertEquals(1, spyClient.getAllShuffleServers(appId).size());
  }

  @Test
  public void stageAwareUnregisterShuffleShouldRejectMissingMapping() {
    ShuffleWriteClientImpl shuffleWriteClient =
        ShuffleClientFactory.newWriteBuilder()
            .clientType(ClientType.GRPC.name())
            .retryMax(1)
            .retryIntervalMax(100)
            .heartBeatThreadNum(1)
            .replica(1)
            .replicaWrite(1)
            .replicaRead(1)
            .replicaSkipEnabled(true)
            .dataTransferPoolSize(1)
            .dataCommitPoolSize(1)
            .unregisterThreadPoolSize(1)
            .unregisterTimeSec(1)
            .unregisterRequestTimeSec(1)
            .build();
    String appId = "stageAwareUnregisterShuffleShouldRejectMissingMapping";

    assertThrows(RssException.class, () -> shuffleWriteClient.unregisterShuffle(appId, 1, 0));
  }

  @Test
  public void stageAwareUnregisterShuffleShouldRemoveMappingAfterCleanupAck() {
    ShuffleWriteClientImpl shuffleWriteClient =
        ShuffleClientFactory.newWriteBuilder()
            .clientType(ClientType.GRPC.name())
            .retryMax(1)
            .retryIntervalMax(100)
            .heartBeatThreadNum(1)
            .replica(1)
            .replicaWrite(1)
            .replicaRead(1)
            .replicaSkipEnabled(true)
            .dataTransferPoolSize(1)
            .dataCommitPoolSize(1)
            .unregisterThreadPoolSize(1)
            .unregisterTimeSec(1)
            .unregisterRequestTimeSec(1)
            .build();
    ShuffleServerClient mockShuffleServerClient = mock(ShuffleServerClient.class);
    ShuffleWriteClientImpl spyClient = Mockito.spy(shuffleWriteClient);
    doReturn(mockShuffleServerClient).when(spyClient).getShuffleServerClient(any());
    when(mockShuffleServerClient.unregisterShuffle(any()))
        .thenReturn(new RssUnregisterShuffleResponse(StatusCode.SUCCESS, true));
    String appId = "stageAwareUnregisterShuffleShouldRemoveMappingAfterCleanupAck";
    ShuffleServerInfo server = new ShuffleServerInfo("server-1", "127.0.0.1", 1);
    spyClient.addShuffleServer(appId, 1, server);

    spyClient.unregisterShuffle(appId, 1, 0);

    assertEquals(0, spyClient.getAllShuffleServers(appId).size());
  }

  @Test
  public void stageAwareSendCommitShouldSetStageAttemptForCommitAndFinish() {
    ShuffleWriteClientImpl shuffleWriteClient =
        ShuffleClientFactory.newWriteBuilder()
            .clientType(ClientType.GRPC.name())
            .retryMax(1)
            .retryIntervalMax(100)
            .heartBeatThreadNum(1)
            .replica(1)
            .replicaWrite(1)
            .replicaRead(1)
            .replicaSkipEnabled(true)
            .dataTransferPoolSize(1)
            .dataCommitPoolSize(1)
            .unregisterThreadPoolSize(1)
            .unregisterTimeSec(1)
            .unregisterRequestTimeSec(1)
            .build();
    ShuffleServerClient mockShuffleServerClient = mock(ShuffleServerClient.class);
    ShuffleWriteClientImpl spyClient = Mockito.spy(shuffleWriteClient);
    doReturn(mockShuffleServerClient).when(spyClient).getShuffleServerClient(any());
    RssSendCommitResponse commitResponse = new RssSendCommitResponse(StatusCode.SUCCESS);
    commitResponse.setCommitCount(1);
    commitResponse.setStageAttemptAccepted(true);
    when(mockShuffleServerClient.sendCommit(any())).thenReturn(commitResponse);
    RssFinishShuffleResponse finishResponse = new RssFinishShuffleResponse(StatusCode.SUCCESS);
    finishResponse.setStageAttemptAccepted(true);
    when(mockShuffleServerClient.finishShuffle(any()))
        .thenReturn(finishResponse);

    Set<ShuffleServerInfo> servers = Sets.newHashSet(new ShuffleServerInfo("id", "127.0.0.1", 1));

    assertTrue(spyClient.sendCommit(servers, "app-1", 2, 1, 3));

    ArgumentCaptor<RssSendCommitRequest> commitCaptor =
        ArgumentCaptor.forClass(RssSendCommitRequest.class);
    verify(mockShuffleServerClient).sendCommit(commitCaptor.capture());
    assertTrue(commitCaptor.getValue().hasStageAttemptNumber());
    assertEquals(3, commitCaptor.getValue().getStageAttemptNumber());

    ArgumentCaptor<RssFinishShuffleRequest> finishCaptor =
        ArgumentCaptor.forClass(RssFinishShuffleRequest.class);
    verify(mockShuffleServerClient).finishShuffle(finishCaptor.capture());
    assertTrue(finishCaptor.getValue().hasStageAttemptNumber());
    assertEquals(3, finishCaptor.getValue().getStageAttemptNumber());
  }

  @ParameterizedTest
  @MethodSource("invalidStageAwareCommitResponses")
  public void stageAwareSendCommitShouldRejectInvalidCommitAck(
      RssSendCommitResponse commitResponse) {
    ShuffleWriteClientImpl shuffleWriteClient =
        ShuffleClientFactory.newWriteBuilder()
            .clientType(ClientType.GRPC.name())
            .retryMax(1)
            .retryIntervalMax(100)
            .heartBeatThreadNum(1)
            .replica(1)
            .replicaWrite(1)
            .replicaRead(1)
            .replicaSkipEnabled(true)
            .dataTransferPoolSize(1)
            .dataCommitPoolSize(1)
            .unregisterThreadPoolSize(1)
            .unregisterTimeSec(1)
            .unregisterRequestTimeSec(1)
            .build();
    ShuffleServerClient mockShuffleServerClient = mock(ShuffleServerClient.class);
    ShuffleWriteClientImpl spyClient = Mockito.spy(shuffleWriteClient);
    doReturn(mockShuffleServerClient).when(spyClient).getShuffleServerClient(any());
    when(mockShuffleServerClient.sendCommit(any())).thenReturn(commitResponse);

    Set<ShuffleServerInfo> servers = Sets.newHashSet(new ShuffleServerInfo("id", "127.0.0.1", 1));

    assertFalse(spyClient.sendCommit(servers, "app-1", 2, 1, 3));
    verify(mockShuffleServerClient, never()).finishShuffle(any());
  }

  private static Stream<Arguments> invalidStageAwareCommitResponses() {
    RssSendCommitResponse missingAck = new RssSendCommitResponse(StatusCode.SUCCESS);
    missingAck.setCommitCount(1);
    RssSendCommitResponse ackFalse = new RssSendCommitResponse(StatusCode.SUCCESS);
    ackFalse.setCommitCount(1);
    ackFalse.setStageAttemptAccepted(false);
    return Stream.of(Arguments.of(missingAck), Arguments.of(ackFalse));
  }

  @ParameterizedTest
  @MethodSource("invalidStageAwareFinishResponses")
  public void stageAwareSendCommitShouldRejectFinishWithoutAcceptedAck(
      RssFinishShuffleResponse finishResponse) {
    ShuffleWriteClientImpl shuffleWriteClient =
        ShuffleClientFactory.newWriteBuilder()
            .clientType(ClientType.GRPC.name())
            .retryMax(1)
            .retryIntervalMax(100)
            .heartBeatThreadNum(1)
            .replica(1)
            .replicaWrite(1)
            .replicaRead(1)
            .replicaSkipEnabled(true)
            .dataTransferPoolSize(1)
            .dataCommitPoolSize(1)
            .unregisterThreadPoolSize(1)
            .unregisterTimeSec(1)
            .unregisterRequestTimeSec(1)
            .build();
    ShuffleServerClient mockShuffleServerClient = mock(ShuffleServerClient.class);
    ShuffleWriteClientImpl spyClient = Mockito.spy(shuffleWriteClient);
    doReturn(mockShuffleServerClient).when(spyClient).getShuffleServerClient(any());
    RssSendCommitResponse commitResponse = new RssSendCommitResponse(StatusCode.SUCCESS);
    commitResponse.setCommitCount(1);
    commitResponse.setStageAttemptAccepted(true);
    when(mockShuffleServerClient.sendCommit(any())).thenReturn(commitResponse);
    when(mockShuffleServerClient.finishShuffle(any())).thenReturn(finishResponse);

    Set<ShuffleServerInfo> servers = Sets.newHashSet(new ShuffleServerInfo("id", "127.0.0.1", 1));

    assertFalse(spyClient.sendCommit(servers, "app-1", 2, 1, 3));
    verify(mockShuffleServerClient).finishShuffle(any());
  }

  private static Stream<Arguments> invalidStageAwareFinishResponses() {
    RssFinishShuffleResponse missingAck = new RssFinishShuffleResponse(StatusCode.SUCCESS);
    RssFinishShuffleResponse ackFalse = new RssFinishShuffleResponse(StatusCode.SUCCESS);
    ackFalse.setStageAttemptAccepted(false);
    return Stream.of(Arguments.of(missingAck), Arguments.of(ackFalse));
  }

  @Test
  public void legacySendCommitShouldKeepStageAttemptAbsent() {
    ShuffleWriteClientImpl shuffleWriteClient =
        ShuffleClientFactory.newWriteBuilder()
            .clientType(ClientType.GRPC.name())
            .retryMax(1)
            .retryIntervalMax(100)
            .heartBeatThreadNum(1)
            .replica(1)
            .replicaWrite(1)
            .replicaRead(1)
            .replicaSkipEnabled(true)
            .dataTransferPoolSize(1)
            .dataCommitPoolSize(1)
            .unregisterThreadPoolSize(1)
            .unregisterTimeSec(1)
            .unregisterRequestTimeSec(1)
            .build();
    ShuffleServerClient mockShuffleServerClient = mock(ShuffleServerClient.class);
    ShuffleWriteClientImpl spyClient = Mockito.spy(shuffleWriteClient);
    doReturn(mockShuffleServerClient).when(spyClient).getShuffleServerClient(any());
    RssSendCommitResponse commitResponse = new RssSendCommitResponse(StatusCode.SUCCESS);
    commitResponse.setCommitCount(0);
    when(mockShuffleServerClient.sendCommit(any())).thenReturn(commitResponse);

    Set<ShuffleServerInfo> servers = Sets.newHashSet(new ShuffleServerInfo("id", "127.0.0.1", 1));

    assertTrue(spyClient.sendCommit(servers, "app-1", 2, 1));

    ArgumentCaptor<RssSendCommitRequest> commitCaptor =
        ArgumentCaptor.forClass(RssSendCommitRequest.class);
    verify(mockShuffleServerClient).sendCommit(commitCaptor.capture());
    assertFalse(commitCaptor.getValue().hasStageAttemptNumber());
  }

  @Test
  public void testSendDataWithDefectiveServers() {
    ShuffleWriteClientImpl shuffleWriteClient =
        ShuffleClientFactory.newWriteBuilder()
            .clientType(ClientType.GRPC.name())
            .retryMax(3)
            .retryIntervalMax(2000)
            .heartBeatThreadNum(4)
            .replica(3)
            .replicaWrite(2)
            .replicaRead(2)
            .replicaSkipEnabled(true)
            .dataTransferPoolSize(1)
            .dataCommitPoolSize(1)
            .unregisterThreadPoolSize(10)
            .unregisterTimeSec(10)
            .unregisterRequestTimeSec(10)
            .build();
    ShuffleServerClient mockShuffleServerClient = mock(ShuffleServerClient.class);
    ShuffleWriteClientImpl spyClient = Mockito.spy(shuffleWriteClient);
    doReturn(mockShuffleServerClient).when(spyClient).getShuffleServerClient(any());
    when(mockShuffleServerClient.sendShuffleData(any()))
        .thenReturn(
            new RssSendShuffleDataResponse(StatusCode.NO_BUFFER),
            new RssSendShuffleDataResponse(StatusCode.SUCCESS),
            new RssSendShuffleDataResponse(StatusCode.SUCCESS));

    String appId = "testSendDataWithDefectiveServers_appId";
    ShuffleServerInfo ssi1 = new ShuffleServerInfo("127.0.0.1", 0);
    ShuffleServerInfo ssi2 = new ShuffleServerInfo("127.0.0.1", 1);
    ShuffleServerInfo ssi3 = new ShuffleServerInfo("127.0.0.1", 2);
    List<ShuffleServerInfo> shuffleServerInfoList = Lists.newArrayList(ssi1, ssi2, ssi3);
    List<ShuffleBlockInfo> shuffleBlockInfoList =
        Lists.newArrayList(
            new ShuffleBlockInfo(
                0, 0, 10, 10, 10, new byte[] {10}, shuffleServerInfoList, 10, 100, 0));
    SendShuffleDataResult result =
        spyClient.sendShuffleData(appId, shuffleBlockInfoList, () -> false);
    assertEquals(0, result.getFailedBlockIds().size());

    // Send data for the second time, the first shuffle server will be moved to the last.
    when(mockShuffleServerClient.sendShuffleData(any()))
        .thenReturn(
            new RssSendShuffleDataResponse(StatusCode.SUCCESS),
            new RssSendShuffleDataResponse(StatusCode.SUCCESS));
    List<ShuffleServerInfo> excludeServers = new ArrayList<>();
    spyClient.genServerToBlocks(
        shuffleBlockInfoList.get(0),
        shuffleServerInfoList,
        2,
        excludeServers,
        Maps.newHashMap(),
        Maps.newHashMap(),
        true);
    assertEquals(2, excludeServers.size());
    assertEquals(ssi2, excludeServers.get(0));
    assertEquals(ssi3, excludeServers.get(1));
    spyClient.genServerToBlocks(
        shuffleBlockInfoList.get(0),
        shuffleServerInfoList,
        1,
        excludeServers,
        Maps.newHashMap(),
        Maps.newHashMap(),
        false);
    assertEquals(3, excludeServers.size());
    assertEquals(ssi1, excludeServers.get(2));
    result = spyClient.sendShuffleData(appId, shuffleBlockInfoList, () -> false);
    assertEquals(0, result.getFailedBlockIds().size());

    // Send data for the third time, the first server will be removed from the defectiveServers
    // and the second server will be added to the defectiveServers.
    when(mockShuffleServerClient.sendShuffleData(any()))
        .thenReturn(
            new RssSendShuffleDataResponse(StatusCode.NO_BUFFER),
            new RssSendShuffleDataResponse(StatusCode.SUCCESS),
            new RssSendShuffleDataResponse(StatusCode.SUCCESS));
    List<ShuffleServerInfo> shuffleServerInfoList2 = Lists.newArrayList(ssi2, ssi1, ssi3);
    List<ShuffleBlockInfo> shuffleBlockInfoList2 =
        Lists.newArrayList(
            new ShuffleBlockInfo(
                0, 0, 10, 10, 10, new byte[] {10}, shuffleServerInfoList2, 10, 100, 0));
    result = spyClient.sendShuffleData(appId, shuffleBlockInfoList2, () -> false);
    assertEquals(0, result.getFailedBlockIds().size());
    assertEquals(1, spyClient.getDefectiveServers().size());
    assertEquals(ssi2, spyClient.getDefectiveServers().toArray()[0]);
    excludeServers = new ArrayList<>();
    spyClient.genServerToBlocks(
        shuffleBlockInfoList.get(0),
        shuffleServerInfoList,
        2,
        excludeServers,
        Maps.newHashMap(),
        Maps.newHashMap(),
        true);
    assertEquals(2, excludeServers.size());
    assertEquals(ssi1, excludeServers.get(0));
    assertEquals(ssi3, excludeServers.get(1));
    spyClient.genServerToBlocks(
        shuffleBlockInfoList.get(0),
        shuffleServerInfoList,
        1,
        excludeServers,
        Maps.newHashMap(),
        Maps.newHashMap(),
        false);
    assertEquals(3, excludeServers.size());
    assertEquals(ssi2, excludeServers.get(2));

    // Check whether it is normal when two shuffle servers in defectiveServers
    spyClient.getDefectiveServers().add(ssi1);
    assertEquals(2, spyClient.getDefectiveServers().size());
    excludeServers = new ArrayList<>();
    spyClient.genServerToBlocks(
        shuffleBlockInfoList.get(0),
        shuffleServerInfoList,
        2,
        excludeServers,
        Maps.newHashMap(),
        Maps.newHashMap(),
        true);
    assertEquals(2, excludeServers.size());
    assertEquals(ssi3, excludeServers.get(0));
    assertEquals(ssi1, excludeServers.get(1));
  }

  @Test
  public void testSettingRssClientConfigs() {
    RssConf rssConf = new RssConf();
    rssConf.set(RssClientConf.NETTY_IO_MODE, IOMode.EPOLL);
    ShuffleClientFactory.WriteClientBuilder writeClientBuilder =
        ShuffleClientFactory.newWriteBuilder()
            .clientType(ClientType.GRPC_NETTY.name())
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
            .rssConf(rssConf);
    ShuffleWriteClientImpl client = writeClientBuilder.build();
    IOMode ioMode = writeClientBuilder.getRssConf().get(RssClientConf.NETTY_IO_MODE);
    client.close();
    assertEquals(IOMode.EPOLL, ioMode);
  }

  public static Stream<Arguments> testBlockIdLayouts() {
    return Stream.of(
        Arguments.of(BlockIdLayout.DEFAULT), Arguments.of(BlockIdLayout.from(20, 21, 22)));
  }

  @ParameterizedTest
  @MethodSource("testBlockIdLayouts")
  public void testGetShuffleResult(BlockIdLayout layout) {
    RssConf rssConf = new RssConf();
    rssConf.set(RssClientConf.BLOCKID_SEQUENCE_NO_BITS, layout.sequenceNoBits);
    rssConf.set(RssClientConf.BLOCKID_PARTITION_ID_BITS, layout.partitionIdBits);
    rssConf.set(RssClientConf.BLOCKID_TASK_ATTEMPT_ID_BITS, layout.taskAttemptIdBits);
    ShuffleWriteClientImpl shuffleWriteClient =
        ShuffleClientFactory.newWriteBuilder()
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
            .rssConf(rssConf)
            .build();
    ShuffleServerClient mockShuffleServerClient = mock(ShuffleServerClient.class);
    ShuffleWriteClientImpl spyClient = Mockito.spy(shuffleWriteClient);
    doReturn(mockShuffleServerClient).when(spyClient).getShuffleServerClient(any());
    RssGetShuffleResultResponse response;
    try {
      Roaring64NavigableMap res = Roaring64NavigableMap.bitmapOf(1L, 2L, 5L);
      response = new RssGetShuffleResultResponse(StatusCode.SUCCESS, RssUtils.serializeBitMap(res));
    } catch (Exception e) {
      throw new RssException(e);
    }
    when(mockShuffleServerClient.getShuffleResult(any())).thenReturn(response);

    Set<ShuffleServerInfo> shuffleServerInfoSet =
        Sets.newHashSet(new ShuffleServerInfo("id", "host", 0));
    Roaring64NavigableMap result =
        spyClient.getShuffleResult("GRPC", shuffleServerInfoSet, "appId", 1, 2);

    verify(mockShuffleServerClient)
        .getShuffleResult(argThat(request -> request.getBlockIdLayout().equals(layout)));
    assertArrayEquals(result.stream().sorted().toArray(), new long[] {1L, 2L, 5L});
  }

  @Test
  public void reportShuffleResultShouldSetStageAttemptNumber() {
    ShuffleServerClient mockShuffleServerClient = mock(ShuffleServerClient.class);
    ShuffleWriteClientImpl spyClient = createSpyClient(mockShuffleServerClient);
    when(mockShuffleServerClient.reportShuffleResult(any()))
        .thenReturn(new RssReportShuffleResultResponse(StatusCode.SUCCESS, true, true));
    Map<ShuffleServerInfo, Map<Integer, Set<Long>>> blockIds = createReportBlockIds();

    spyClient.reportShuffleResult(
        blockIds, "appId", 1, 2L, 3, 4, Sets.newHashSet(), false, createRecordNumbers(blockIds));

    ArgumentCaptor<RssReportShuffleResultRequest> captor =
        ArgumentCaptor.forClass(RssReportShuffleResultRequest.class);
    verify(mockShuffleServerClient).reportShuffleResult(captor.capture());
    RssReportShuffleResultRequest request = captor.getValue();
    assertEquals(4, request.getStageAttemptNumber());
    assertTrue(request.hasStageAttemptNumber());
    assertTrue(request.toProto().hasStageAttemptNumber());
  }

  @Test
  public void reportShuffleResultShouldSetStageAttemptNumberWhenZero() {
    ShuffleServerClient mockShuffleServerClient = mock(ShuffleServerClient.class);
    ShuffleWriteClientImpl spyClient = createSpyClient(mockShuffleServerClient);
    when(mockShuffleServerClient.reportShuffleResult(any()))
        .thenReturn(new RssReportShuffleResultResponse(StatusCode.SUCCESS, true, true));
    Map<ShuffleServerInfo, Map<Integer, Set<Long>>> blockIds = createReportBlockIds();

    spyClient.reportShuffleResult(
        blockIds, "appId", 1, 2L, 3, 0, Sets.newHashSet(), false, createRecordNumbers(blockIds));

    ArgumentCaptor<RssReportShuffleResultRequest> captor =
        ArgumentCaptor.forClass(RssReportShuffleResultRequest.class);
    verify(mockShuffleServerClient).reportShuffleResult(captor.capture());
    RssReportShuffleResultRequest request = captor.getValue();
    assertEquals(0, request.getStageAttemptNumber());
    assertTrue(request.hasStageAttemptNumber());
    assertTrue(request.toProto().hasStageAttemptNumber());
  }

  @Test
  public void reportShuffleResultWithoutRecordNumbersShouldSetStageAttemptNumber() {
    ShuffleServerClient mockShuffleServerClient = mock(ShuffleServerClient.class);
    ShuffleWriteClientImpl spyClient = createSpyClient(mockShuffleServerClient);
    when(mockShuffleServerClient.reportShuffleResult(any()))
        .thenReturn(new RssReportShuffleResultResponse(StatusCode.SUCCESS, true, true));

    spyClient.reportShuffleResult(
        createReportBlockIds(), "appId", 1, 2L, 3, 4, Sets.newHashSet(), false);

    ArgumentCaptor<RssReportShuffleResultRequest> captor =
        ArgumentCaptor.forClass(RssReportShuffleResultRequest.class);
    verify(mockShuffleServerClient).reportShuffleResult(captor.capture());
    RssReportShuffleResultRequest request = captor.getValue();
    assertEquals(4, request.getStageAttemptNumber());
    assertTrue(request.hasStageAttemptNumber());
    assertTrue(request.toProto().hasStageAttemptNumber());
  }

  @Test
  public void reportShuffleResultWithoutRecordNumbersShouldSetStageAttemptNumberWhenZero() {
    ShuffleServerClient mockShuffleServerClient = mock(ShuffleServerClient.class);
    ShuffleWriteClientImpl spyClient = createSpyClient(mockShuffleServerClient);
    when(mockShuffleServerClient.reportShuffleResult(any()))
        .thenReturn(new RssReportShuffleResultResponse(StatusCode.SUCCESS, true, true));

    spyClient.reportShuffleResult(
        createReportBlockIds(), "appId", 1, 2L, 3, 0, Sets.newHashSet(), false);

    ArgumentCaptor<RssReportShuffleResultRequest> captor =
        ArgumentCaptor.forClass(RssReportShuffleResultRequest.class);
    verify(mockShuffleServerClient).reportShuffleResult(captor.capture());
    RssReportShuffleResultRequest request = captor.getValue();
    assertEquals(0, request.getStageAttemptNumber());
    assertTrue(request.hasStageAttemptNumber());
    assertTrue(request.toProto().hasStageAttemptNumber());
  }

  @Test
  public void reportShuffleResultOldOverloadShouldNotSetStageAttemptNumber() {
    ShuffleServerClient mockShuffleServerClient = mock(ShuffleServerClient.class);
    ShuffleWriteClientImpl spyClient = createSpyClient(mockShuffleServerClient);
    when(mockShuffleServerClient.reportShuffleResult(any()))
        .thenReturn(new RssReportShuffleResultResponse(StatusCode.SUCCESS));

    spyClient.reportShuffleResult(createReportBlockIds(), "appId", 1, 2L, 3);

    ArgumentCaptor<RssReportShuffleResultRequest> captor =
        ArgumentCaptor.forClass(RssReportShuffleResultRequest.class);
    verify(mockShuffleServerClient).reportShuffleResult(captor.capture());
    RssReportShuffleResultRequest request = captor.getValue();
    assertEquals(0, request.getStageAttemptNumber());
    assertFalse(request.hasStageAttemptNumber());
    assertFalse(request.toProto().hasStageAttemptNumber());
  }

  @Test
  public void reportShuffleResultOldRecordNumbersOverloadShouldNotSetStageAttemptNumber() {
    ShuffleServerClient mockShuffleServerClient = mock(ShuffleServerClient.class);
    ShuffleWriteClientImpl spyClient = createSpyClient(mockShuffleServerClient);
    when(mockShuffleServerClient.reportShuffleResult(any()))
        .thenReturn(new RssReportShuffleResultResponse(StatusCode.SUCCESS));
    Map<ShuffleServerInfo, Map<Integer, Set<Long>>> blockIds = createReportBlockIds();

    spyClient.reportShuffleResult(
        blockIds,
        "appId",
        1,
        2L,
        3,
        Sets.newHashSet(),
        false,
        createRecordNumbers(blockIds));

    ArgumentCaptor<RssReportShuffleResultRequest> captor =
        ArgumentCaptor.forClass(RssReportShuffleResultRequest.class);
    verify(mockShuffleServerClient).reportShuffleResult(captor.capture());
    RssReportShuffleResultRequest request = captor.getValue();
    assertEquals(0, request.getStageAttemptNumber());
    assertFalse(request.hasStageAttemptNumber());
    assertFalse(request.toProto().hasStageAttemptNumber());
    assertEquals(1, request.toProto().getPartitionStatsCount());
  }

  @Test
  public void stageAwareReportShuffleResultShouldRejectSuccessWithoutAck() {
    ShuffleServerClient mockShuffleServerClient = mock(ShuffleServerClient.class);
    ShuffleWriteClientImpl spyClient = createSpyClient(mockShuffleServerClient);
    when(mockShuffleServerClient.reportShuffleResult(any()))
        .thenReturn(new RssReportShuffleResultResponse(StatusCode.SUCCESS));

    assertThrows(
        RssException.class,
        () ->
            spyClient.reportShuffleResult(
                createReportBlockIds(), "appId", 1, 2L, 3, 0, Sets.newHashSet(), false));
  }

  @Test
  public void stageAwareReportShuffleResultShouldNotTreatStaleFenceAsSendFailure() {
    ShuffleServerClient mockShuffleServerClient = mock(ShuffleServerClient.class);
    ShuffleWriteClientImpl spyClient = createSpyClient(mockShuffleServerClient);
    when(mockShuffleServerClient.reportShuffleResult(any()))
        .thenReturn(new RssReportShuffleResultResponse(StatusCode.STAGE_RETRY_IGNORE));
    Set<ShuffleServerInfo> reportFailureServers = Sets.newHashSet();

    RssException exception =
        assertThrows(
            RssException.class,
            () ->
                spyClient.reportShuffleResult(
                    createReportBlockIds(), "appId", 1, 2L, 3, 0, reportFailureServers, true));

    assertFalse(exception instanceof RssSendFailedException);
    assertTrue(reportFailureServers.isEmpty());
  }

  @Test
  public void stageAwareStartSortMergeShouldRejectSuccessWithoutAck() {
    ShuffleServerClient mockShuffleServerClient = mock(ShuffleServerClient.class);
    ShuffleWriteClientImpl spyClient = createSpyClient(mockShuffleServerClient);
    when(mockShuffleServerClient.startSortMerge(any()))
        .thenReturn(new RssStartSortMergeResponse(StatusCode.SUCCESS));
    Set<ShuffleServerInfo> servers = Sets.newHashSet(new ShuffleServerInfo("id", "127.0.0.1", 1));

    assertThrows(
        RssFetchFailedException.class,
        () -> spyClient.startSortMerge(servers, "appId", 1, 0, Roaring64NavigableMap.bitmapOf(), 0));
  }

  @Test
  public void startSortMergeShouldContinueAfterPerServerException() {
    ShuffleServerClient failedClient = mock(ShuffleServerClient.class);
    ShuffleServerClient successClient = mock(ShuffleServerClient.class);
    ShuffleWriteClientImpl shuffleWriteClient =
        ShuffleClientFactory.newWriteBuilder()
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
            .build();
    ShuffleWriteClientImpl spyClient = Mockito.spy(shuffleWriteClient);
    ShuffleServerInfo failedServer = new ShuffleServerInfo("failed", "127.0.0.1", 1);
    ShuffleServerInfo successServer = new ShuffleServerInfo("success", "127.0.0.2", 2);
    doReturn(failedClient).when(spyClient).getShuffleServerClient(failedServer);
    doReturn(successClient).when(spyClient).getShuffleServerClient(successServer);
    when(failedClient.startSortMerge(any())).thenThrow(new RuntimeException("first failed"));
    RssStartSortMergeResponse successResponse = new RssStartSortMergeResponse(StatusCode.SUCCESS);
    successResponse.setStageAttemptAccepted(true);
    when(successClient.startSortMerge(any())).thenReturn(successResponse);

    spyClient.startSortMerge(
        Sets.newLinkedHashSet(Lists.newArrayList(failedServer, successServer)),
        "appId",
        1,
        0,
        Roaring64NavigableMap.bitmapOf(),
        0);

    verify(failedClient).startSortMerge(any());
    verify(successClient).startSortMerge(any());
  }

  private ShuffleWriteClientImpl createSpyClient(ShuffleServerClient mockShuffleServerClient) {
    ShuffleWriteClientImpl shuffleWriteClient =
        ShuffleClientFactory.newWriteBuilder()
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
            .build();
    ShuffleWriteClientImpl spyClient = Mockito.spy(shuffleWriteClient);
    doReturn(mockShuffleServerClient).when(spyClient).getShuffleServerClient(any());
    return spyClient;
  }

  private Map<ShuffleServerInfo, Map<Integer, Set<Long>>> createReportBlockIds() {
    ShuffleServerInfo server = new ShuffleServerInfo("id", "host", 0);
    Map<Integer, Set<Long>> partitionToBlockIds = Maps.newHashMap();
    partitionToBlockIds.put(0, Sets.newHashSet(10L));
    Map<ShuffleServerInfo, Map<Integer, Set<Long>>> serverToPartitionToBlockIds =
        Maps.newHashMap();
    serverToPartitionToBlockIds.put(server, partitionToBlockIds);
    return serverToPartitionToBlockIds;
  }

  private Map<ShuffleServerInfo, Map<Integer, Long>> createRecordNumbers(
      Map<ShuffleServerInfo, Map<Integer, Set<Long>>> blockIds) {
    Map<ShuffleServerInfo, Map<Integer, Long>> recordNumbers = Maps.newHashMap();
    for (Map.Entry<ShuffleServerInfo, Map<Integer, Set<Long>>> serverEntry : blockIds.entrySet()) {
      Map<Integer, Long> partitionToRecordNumbers = Maps.newHashMap();
      for (Map.Entry<Integer, Set<Long>> partitionEntry : serverEntry.getValue().entrySet()) {
        partitionToRecordNumbers.put(
            partitionEntry.getKey(), (long) partitionEntry.getValue().size());
      }
      recordNumbers.put(serverEntry.getKey(), partitionToRecordNumbers);
    }
    return recordNumbers;
  }
}
