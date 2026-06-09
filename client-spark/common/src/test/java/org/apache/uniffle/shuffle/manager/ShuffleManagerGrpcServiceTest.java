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

package org.apache.uniffle.shuffle.manager;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;
import org.mockito.Mockito;

import org.apache.uniffle.proto.RssProtos.PartitionToBlockIds;
import org.apache.uniffle.proto.RssProtos.ReportShuffleFetchFailureRequest;
import org.apache.uniffle.proto.RssProtos.ReportShuffleFetchFailureResponse;
import org.apache.uniffle.proto.RssProtos.ReportShuffleResultRequest;
import org.apache.uniffle.proto.RssProtos.ReportShuffleResultResponse;
import org.apache.uniffle.proto.RssProtos.ReportShuffleWriteFailureRequest;
import org.apache.uniffle.proto.RssProtos.ReportShuffleWriteFailureResponse;
import org.apache.uniffle.proto.RssProtos.ShuffleServerId;
import org.apache.uniffle.proto.RssProtos.StatusCode;
import org.apache.uniffle.shuffle.BlockIdManager;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class ShuffleManagerGrpcServiceTest {
  // create mock of RssShuffleManagerInterface.
  private static RssShuffleManagerBase mockShuffleManager;
  private static final String appId = "app-123";
  private static final int maxFetchFailures = 2;
  private static final int shuffleId = 0;
  private static final int numMaps = 100;
  private static final int numReduces = 10;

  private static class MockedStreamObserver<T> implements StreamObserver<T> {
    T value;
    Throwable error;
    boolean completed;

    @Override
    public void onNext(T value) {
      this.value = value;
    }

    @Override
    public void onError(Throwable t) {
      this.error = t;
    }

    @Override
    public void onCompleted() {
      this.completed = true;
    }
  }

  private static class BlockingBlockIdManager extends BlockIdManager {
    private final AtomicBoolean blockNextAdd = new AtomicBoolean(false);
    private final CountDownLatch addStarted = new CountDownLatch(1);
    private final CountDownLatch releaseAdd = new CountDownLatch(1);

    @Override
    public void add(int shuffleId, int partitionId, List<Long> ids) {
      if (blockNextAdd.compareAndSet(true, false)) {
        addStarted.countDown();
        try {
          assertTrue(releaseAdd.await(10, TimeUnit.SECONDS));
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          fail(e);
        }
      }
      super.add(shuffleId, partitionId, ids);
    }

    void blockNextAdd() {
      blockNextAdd.set(true);
    }

    void awaitAddStarted() throws InterruptedException {
      assertTrue(addStarted.await(10, TimeUnit.SECONDS));
    }

    void releaseAdd() {
      releaseAdd.countDown();
    }

    boolean isAddReleased() {
      return releaseAdd.getCount() == 0;
    }
  }

  @BeforeAll
  public static void setup() {
    mockShuffleManager = mock(RssShuffleManagerBase.class);
    Mockito.when(mockShuffleManager.getAppId()).thenReturn(appId);
    Mockito.when(mockShuffleManager.getNumMaps(shuffleId)).thenReturn(numMaps);
    Mockito.when(mockShuffleManager.getPartitionNum(shuffleId)).thenReturn(numReduces);
    Mockito.when(mockShuffleManager.getMaxFetchFailures()).thenReturn(maxFetchFailures);
  }

  @Test
  public void testShuffleManagerGrpcService() {
    ShuffleManagerGrpcService service = new ShuffleManagerGrpcService(mockShuffleManager);
    MockedStreamObserver<ReportShuffleFetchFailureResponse> appIdResponseObserver =
        new MockedStreamObserver<>();
    ReportShuffleFetchFailureRequest req =
        ReportShuffleFetchFailureRequest.newBuilder()
            .setAppId(appId)
            .setShuffleId(shuffleId)
            .setStageAttemptId(1)
            .setPartitionId(1)
            .buildPartial();

    service.reportShuffleFetchFailure(req, appIdResponseObserver);
    assertTrue(appIdResponseObserver.completed);
    // the first call of ReportShuffleFetchFailureRequest should be successful.
    assertEquals(StatusCode.SUCCESS, appIdResponseObserver.value.getStatus());
    assertFalse(appIdResponseObserver.value.getReSubmitWholeStage());

    // req with wrong appId should fail.
    req =
        ReportShuffleFetchFailureRequest.newBuilder()
            .mergeFrom(req)
            .setAppId("wrong-app-id")
            .build();
    service.reportShuffleFetchFailure(req, appIdResponseObserver);
    assertEquals(StatusCode.INVALID_REQUEST, appIdResponseObserver.value.getStatus());
    // forwards the stageAttemptId to 1 to mock invalid request
    req =
        ReportShuffleFetchFailureRequest.newBuilder()
            .mergeFrom(req)
            .setAppId(appId)
            .setStageAttemptId(0)
            .build();
    service.reportShuffleFetchFailure(req, appIdResponseObserver);
    assertEquals(StatusCode.INVALID_REQUEST, appIdResponseObserver.value.getStatus());
    assertTrue(appIdResponseObserver.value.getMsg().contains("old stage"));

    // reportShuffleWriteFailure with an empty list of shuffleServerIds
    MockedStreamObserver<ReportShuffleWriteFailureResponse>
        reportShuffleWriteFailureResponseObserver = new MockedStreamObserver<>();
    ReportShuffleWriteFailureRequest reportShuffleWriteFailureRequest =
        ReportShuffleWriteFailureRequest.newBuilder()
            .setAppId(appId)
            .setShuffleId(shuffleId)
            .buildPartial();
    service.reportShuffleWriteFailure(
        reportShuffleWriteFailureRequest, reportShuffleWriteFailureResponseObserver);
    assertEquals(StatusCode.SUCCESS, reportShuffleWriteFailureResponseObserver.value.getStatus());
  }

  @Test
  public void reportShuffleWriteFailureShouldCleanShuffleDataOnceAfterThreshold()
      throws Exception {
    Mockito.clearInvocations(mockShuffleManager);

    ShuffleManagerGrpcService service = new ShuffleManagerGrpcService(mockShuffleManager);
    MockedStreamObserver<ReportShuffleWriteFailureResponse> responseObserver =
        new MockedStreamObserver<>();
    ReportShuffleWriteFailureRequest request =
        ReportShuffleWriteFailureRequest.newBuilder()
            .setAppId(appId)
            .setShuffleId(shuffleId)
            .setStageAttemptId(1)
            .setStageAttemptNumber(0)
            .addShuffleServerIds(
                ShuffleServerId.newBuilder()
                    .setId("server-1")
                    .setIp("127.0.0.1")
                    .setPort(19999))
            .build();

    for (int i = 0; i < maxFetchFailures; i++) {
      service.reportShuffleWriteFailure(request, responseObserver);
      assertEquals(StatusCode.SUCCESS, responseObserver.value.getStatus());
      assertFalse(responseObserver.value.getReSubmitWholeStage());
      verify(mockShuffleManager, never()).unregisterAllMapOutput(shuffleId);
      verify(mockShuffleManager, never()).unregisterShuffleDataForWriteFailure(shuffleId, 0);
    }

    service.reportShuffleWriteFailure(request, responseObserver);

    assertEquals(StatusCode.SUCCESS, responseObserver.value.getStatus());
    assertTrue(responseObserver.value.getReSubmitWholeStage());
    verify(mockShuffleManager, times(1)).unregisterAllMapOutput(shuffleId);
    verify(mockShuffleManager, times(1)).unregisterShuffleDataForWriteFailure(shuffleId, 0);
    verify(mockShuffleManager, times(1)).clearShuffleDataForWriteFailure(shuffleId);
    InOrder cleanupOrder = Mockito.inOrder(mockShuffleManager);
    cleanupOrder.verify(mockShuffleManager).unregisterAllMapOutput(shuffleId);
    cleanupOrder.verify(mockShuffleManager).unregisterShuffleDataForWriteFailure(shuffleId, 0);
    cleanupOrder.verify(mockShuffleManager).clearShuffleDataForWriteFailure(shuffleId);
    verify(mockShuffleManager, never()).unregisterShuffle(shuffleId);

    service.reportShuffleWriteFailure(request, responseObserver);

    assertEquals(StatusCode.SUCCESS, responseObserver.value.getStatus());
    assertTrue(responseObserver.value.getReSubmitWholeStage());
    verify(mockShuffleManager, times(1)).unregisterAllMapOutput(shuffleId);
    verify(mockShuffleManager, times(1)).unregisterShuffleDataForWriteFailure(shuffleId, 0);
    verify(mockShuffleManager, times(1)).clearShuffleDataForWriteFailure(shuffleId);
    verify(mockShuffleManager, never()).unregisterShuffle(shuffleId);
  }

  @Test
  public void reportShuffleWriteFailureShouldRetryStageWhenServerCleanupFailsAfterMapOutput()
      throws Exception {
    RssShuffleManagerBase shuffleManager = mock(RssShuffleManagerBase.class);
    Mockito.when(shuffleManager.getAppId()).thenReturn(appId);
    Mockito.when(shuffleManager.getMaxFetchFailures()).thenReturn(maxFetchFailures);
    Mockito.doThrow(new RuntimeException("cleanup failed"))
        .when(shuffleManager)
        .unregisterShuffleDataForWriteFailure(shuffleId, 0);
    ShuffleManagerGrpcService service = new ShuffleManagerGrpcService(shuffleManager);
    MockedStreamObserver<ReportShuffleWriteFailureResponse> responseObserver =
        new MockedStreamObserver<>();
    ReportShuffleWriteFailureRequest request =
        ReportShuffleWriteFailureRequest.newBuilder()
            .setAppId(appId)
            .setShuffleId(shuffleId)
            .setStageAttemptId(1)
            .setStageAttemptNumber(0)
            .addShuffleServerIds(
                ShuffleServerId.newBuilder()
                    .setId("server-1")
                    .setIp("127.0.0.1")
                    .setPort(19999))
            .build();

    for (int i = 0; i <= maxFetchFailures; i++) {
      service.reportShuffleWriteFailure(request, responseObserver);
    }

    assertEquals(StatusCode.INTERNAL_ERROR, responseObserver.value.getStatus());
    assertTrue(responseObserver.value.getReSubmitWholeStage());
    verify(shuffleManager, times(1)).unregisterAllMapOutput(shuffleId);
    verify(shuffleManager, times(1)).unregisterShuffleDataForWriteFailure(shuffleId, 0);
    verify(shuffleManager, times(1)).clearShuffleDataForWriteFailure(shuffleId);
    InOrder cleanupOrder = Mockito.inOrder(shuffleManager);
    cleanupOrder.verify(shuffleManager).unregisterAllMapOutput(shuffleId);
    cleanupOrder.verify(shuffleManager).unregisterShuffleDataForWriteFailure(shuffleId, 0);
    cleanupOrder.verify(shuffleManager).clearShuffleDataForWriteFailure(shuffleId);
  }

  @Test
  public void reportShuffleWriteFailureShouldNotClearLocalCacheWhenMapOutputCleanupFails()
      throws Exception {
    int shuffleId = 1;
    BlockIdManager blockIdManager = new BlockIdManager();
    blockIdManager.add(shuffleId, 0, Arrays.asList(1L));
    RssShuffleManagerBase shuffleManager = mock(RssShuffleManagerBase.class);
    Mockito.when(shuffleManager.getAppId()).thenReturn(appId);
    Mockito.when(shuffleManager.getMaxFetchFailures()).thenReturn(maxFetchFailures);
    Mockito.when(shuffleManager.getBlockIdManager()).thenReturn(blockIdManager);
    Mockito.doThrow(new RuntimeException("map output cleanup failed"))
        .when(shuffleManager)
        .unregisterAllMapOutput(shuffleId);
    Mockito.doAnswer(
            invocation -> {
              blockIdManager.remove(shuffleId);
              return null;
            })
        .when(shuffleManager)
        .clearShuffleDataForWriteFailure(shuffleId);
    ShuffleManagerGrpcService service = new ShuffleManagerGrpcService(shuffleManager);
    MockedStreamObserver<ReportShuffleWriteFailureResponse> responseObserver =
        new MockedStreamObserver<>();
    ReportShuffleWriteFailureRequest request =
        ReportShuffleWriteFailureRequest.newBuilder()
            .setAppId(appId)
            .setShuffleId(shuffleId)
            .setStageAttemptId(1)
            .setStageAttemptNumber(0)
            .addShuffleServerIds(
                ShuffleServerId.newBuilder()
                    .setId("server-1")
                    .setIp("127.0.0.1")
                    .setPort(19999))
            .build();

    for (int i = 0; i <= maxFetchFailures; i++) {
      service.reportShuffleWriteFailure(request, responseObserver);
    }

    assertEquals(StatusCode.INTERNAL_ERROR, responseObserver.value.getStatus());
    assertFalse(responseObserver.value.getReSubmitWholeStage());
    assertEquals(1, blockIdManager.get(shuffleId, 0).getLongCardinality());
    verify(shuffleManager, times(1)).unregisterAllMapOutput(shuffleId);
    verify(shuffleManager, never()).unregisterShuffleDataForWriteFailure(shuffleId, 0);
    verify(shuffleManager, never()).clearShuffleDataForWriteFailure(shuffleId);
  }

  @Test
  public void reportShuffleResultShouldIgnoreStaleAttemptAfterWriteFailureCleanup()
      throws Exception {
    int shuffleId = 1;
    BlockIdManager blockIdManager = new BlockIdManager();
    RssShuffleManagerBase shuffleManager = mock(RssShuffleManagerBase.class);
    Mockito.when(shuffleManager.getAppId()).thenReturn(appId);
    Mockito.when(shuffleManager.getMaxFetchFailures()).thenReturn(maxFetchFailures);
    Mockito.when(shuffleManager.getBlockIdManager()).thenReturn(blockIdManager);

    final ShuffleManagerGrpcService service = new ShuffleManagerGrpcService(shuffleManager);
    MockedStreamObserver<ReportShuffleResultResponse> resultResponseObserver =
        new MockedStreamObserver<>();
    ReportShuffleResultRequest staleResultRequest =
        ReportShuffleResultRequest.newBuilder()
            .setAppId(appId)
            .setShuffleId(shuffleId)
            .setStageAttemptNumber(0)
            .addPartitionToBlockIds(
                PartitionToBlockIds.newBuilder()
                    .setPartitionId(0)
                    .addAllBlockIds(Arrays.asList(1L, 2L)))
            .build();
    Mockito.doAnswer(
            invocation -> {
              blockIdManager.remove(shuffleId);
              service.reportShuffleResult(staleResultRequest, new MockedStreamObserver<>());
              return null;
            })
        .when(shuffleManager)
        .clearShuffleDataForWriteFailure(shuffleId);

    service.reportShuffleResult(staleResultRequest, resultResponseObserver);
    assertEquals(StatusCode.SUCCESS, resultResponseObserver.value.getStatus());
    assertTrue(resultResponseObserver.value.hasStageAttemptAccepted());
    assertTrue(resultResponseObserver.value.getStageAttemptAccepted());
    assertEquals(2, blockIdManager.get(shuffleId, 0).getLongCardinality());

    MockedStreamObserver<ReportShuffleWriteFailureResponse> failureResponseObserver =
        new MockedStreamObserver<>();
    ReportShuffleWriteFailureRequest failureRequest =
        ReportShuffleWriteFailureRequest.newBuilder()
            .setAppId(appId)
            .setShuffleId(shuffleId)
            .setStageAttemptId(1)
            .setStageAttemptNumber(0)
            .addShuffleServerIds(
                ShuffleServerId.newBuilder()
                    .setId("server-1")
                    .setIp("127.0.0.1")
                    .setPort(19999))
            .build();
    for (int i = 0; i <= maxFetchFailures; i++) {
      service.reportShuffleWriteFailure(failureRequest, failureResponseObserver);
    }
    assertEquals(StatusCode.SUCCESS, failureResponseObserver.value.getStatus());
    assertTrue(failureResponseObserver.value.getReSubmitWholeStage());
    assertEquals(0, blockIdManager.get(shuffleId, 0).getLongCardinality());

    service.reportShuffleResult(staleResultRequest, resultResponseObserver);
    assertEquals(StatusCode.STAGE_RETRY_IGNORE, resultResponseObserver.value.getStatus());
    assertTrue(resultResponseObserver.value.hasStageAttemptAccepted());
    assertFalse(resultResponseObserver.value.getStageAttemptAccepted());
    assertEquals(0, blockIdManager.get(shuffleId, 0).getLongCardinality());

    ReportShuffleResultRequest missingStageAttemptResultRequest =
        ReportShuffleResultRequest.newBuilder()
            .setAppId(appId)
            .setShuffleId(shuffleId)
            .addPartitionToBlockIds(
                PartitionToBlockIds.newBuilder().setPartitionId(0).addBlockIds(4L))
            .build();
    service.reportShuffleResult(missingStageAttemptResultRequest, resultResponseObserver);
    assertEquals(StatusCode.STAGE_RETRY_IGNORE, resultResponseObserver.value.getStatus());
    assertFalse(resultResponseObserver.value.hasStageAttemptAccepted());
    assertEquals(0, blockIdManager.get(shuffleId, 0).getLongCardinality());

    ReportShuffleWriteFailureRequest nextAttemptFailureRequest =
        ReportShuffleWriteFailureRequest.newBuilder()
            .mergeFrom(failureRequest)
            .setStageAttemptNumber(1)
            .build();
    service.reportShuffleWriteFailure(nextAttemptFailureRequest, failureResponseObserver);
    assertEquals(StatusCode.SUCCESS, failureResponseObserver.value.getStatus());
    assertFalse(failureResponseObserver.value.getReSubmitWholeStage());

    service.reportShuffleResult(missingStageAttemptResultRequest, resultResponseObserver);
    assertEquals(StatusCode.STAGE_RETRY_IGNORE, resultResponseObserver.value.getStatus());
    assertFalse(resultResponseObserver.value.hasStageAttemptAccepted());
    assertEquals(0, blockIdManager.get(shuffleId, 0).getLongCardinality());

    ReportShuffleResultRequest newAttemptResultRequest =
        ReportShuffleResultRequest.newBuilder()
            .setAppId(appId)
            .setShuffleId(shuffleId)
            .setStageAttemptNumber(1)
            .addPartitionToBlockIds(
                PartitionToBlockIds.newBuilder()
                    .setPartitionId(0)
                    .addBlockIds(3L))
            .build();
    service.reportShuffleResult(newAttemptResultRequest, resultResponseObserver);
    assertEquals(StatusCode.SUCCESS, resultResponseObserver.value.getStatus());
    assertTrue(resultResponseObserver.value.hasStageAttemptAccepted());
    assertTrue(resultResponseObserver.value.getStageAttemptAccepted());
    assertEquals(1, blockIdManager.get(shuffleId, 0).getLongCardinality());
    verify(shuffleManager, times(1)).unregisterShuffleDataForWriteFailure(shuffleId, 0);

    service.unregisterShuffle(shuffleId);
    blockIdManager.remove(shuffleId);
    service.reportShuffleResult(staleResultRequest, resultResponseObserver);
    assertEquals(StatusCode.SUCCESS, resultResponseObserver.value.getStatus());
    assertTrue(resultResponseObserver.value.hasStageAttemptAccepted());
    assertTrue(resultResponseObserver.value.getStageAttemptAccepted());
    assertEquals(2, blockIdManager.get(shuffleId, 0).getLongCardinality());
  }

  @Test
  public void reportShuffleResultShouldNotReAddStaleBlocksAfterConcurrentCleanup()
      throws Exception {
    int shuffleId = 2;
    BlockingBlockIdManager blockIdManager = new BlockingBlockIdManager();
    RssShuffleManagerBase shuffleManager = mock(RssShuffleManagerBase.class);
    Mockito.when(shuffleManager.getAppId()).thenReturn(appId);
    Mockito.when(shuffleManager.getMaxFetchFailures()).thenReturn(maxFetchFailures);
    Mockito.when(shuffleManager.getBlockIdManager()).thenReturn(blockIdManager);
    CountDownLatch cleanupStarted = new CountDownLatch(1);
    Mockito.doAnswer(
            invocation -> {
              assertTrue(blockIdManager.isAddReleased());
              cleanupStarted.countDown();
              blockIdManager.remove(shuffleId);
              return null;
            })
        .when(shuffleManager)
        .clearShuffleDataForWriteFailure(shuffleId);

    ShuffleManagerGrpcService service = new ShuffleManagerGrpcService(shuffleManager);
    ReportShuffleResultRequest staleResultRequest =
        ReportShuffleResultRequest.newBuilder()
            .setAppId(appId)
            .setShuffleId(shuffleId)
            .setStageAttemptNumber(0)
            .addPartitionToBlockIds(
                PartitionToBlockIds.newBuilder()
                    .setPartitionId(0)
                    .addAllBlockIds(Arrays.asList(1L, 2L)))
            .build();
    ReportShuffleWriteFailureRequest failureRequest =
        ReportShuffleWriteFailureRequest.newBuilder()
            .setAppId(appId)
            .setShuffleId(shuffleId)
            .setStageAttemptId(1)
            .setStageAttemptNumber(0)
            .addShuffleServerIds(
                ShuffleServerId.newBuilder()
                    .setId("server-1")
                    .setIp("127.0.0.1")
                    .setPort(19999))
            .build();

    service.reportShuffleResult(staleResultRequest, new MockedStreamObserver<>());
    assertEquals(2, blockIdManager.get(shuffleId, 0).getLongCardinality());

    blockIdManager.blockNextAdd();
    CompletableFuture<Void> reportFuture =
        CompletableFuture.runAsync(
            () -> service.reportShuffleResult(staleResultRequest, new MockedStreamObserver<>()));
    blockIdManager.awaitAddStarted();

    CountDownLatch failureStarted = new CountDownLatch(1);
    AtomicReference<Throwable> failureThreadError = new AtomicReference<>();
    Thread failureThread =
        new Thread(
            () -> {
              try {
                failureStarted.countDown();
                MockedStreamObserver<ReportShuffleWriteFailureResponse> failureObserver =
                    new MockedStreamObserver<>();
                for (int i = 0; i <= maxFetchFailures; i++) {
                  service.reportShuffleWriteFailure(failureRequest, failureObserver);
                }
                assertTrue(failureObserver.value.getReSubmitWholeStage());
              } catch (Throwable t) {
                failureThreadError.set(t);
              }
            });
    failureThread.start();

    assertTrue(failureStarted.await(10, TimeUnit.SECONDS));
    blockIdManager.releaseAdd();
    reportFuture.get(10, TimeUnit.SECONDS);
    failureThread.join(TimeUnit.SECONDS.toMillis(10));
    assertFalse(failureThread.isAlive());
    if (failureThreadError.get() != null) {
      throw new AssertionError(failureThreadError.get());
    }
    assertTrue(cleanupStarted.await(10, TimeUnit.SECONDS));

    assertEquals(0, blockIdManager.get(shuffleId, 0).getLongCardinality());
  }
}
