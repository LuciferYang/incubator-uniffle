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

package org.apache.spark.shuffle;

import java.lang.reflect.Field;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import io.grpc.stub.StreamObserver;
import scala.Option;

import org.apache.spark.Partitioner;
import org.apache.spark.ShuffleDependency;
import org.apache.spark.SparkConf;
import org.apache.spark.TaskContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.executor.TaskMetrics;
import org.apache.spark.shuffle.handle.ShuffleHandleInfo;
import org.apache.spark.shuffle.handle.SimpleShuffleHandleInfo;
import org.apache.spark.serializer.KryoSerializer;
import org.apache.spark.sql.internal.SQLConf;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.roaringbitmap.longlong.Roaring64NavigableMap;

import org.apache.uniffle.client.api.ShuffleResult;
import org.apache.uniffle.client.api.ShuffleWriteClient;
import org.apache.uniffle.client.impl.FailedBlockSendTracker;
import org.apache.uniffle.client.util.RssClientConfig;
import org.apache.uniffle.common.RemoteStorageInfo;
import org.apache.uniffle.common.ShuffleDataDistributionType;
import org.apache.uniffle.common.ShuffleServerInfo;
import org.apache.uniffle.common.config.ConfigOption;
import org.apache.uniffle.common.config.RssClientConf;
import org.apache.uniffle.common.exception.RssException;
import org.apache.uniffle.common.rpc.StatusCode;
import org.apache.uniffle.common.util.BlockIdLayout;
import org.apache.uniffle.common.util.JavaUtils;
import org.apache.uniffle.proto.RssProtos;
import org.apache.uniffle.shuffle.manager.RssShuffleManagerBase;
import org.apache.uniffle.shuffle.manager.ShuffleManagerGrpcService;
import org.apache.uniffle.storage.util.StorageType;

import static org.apache.spark.shuffle.RssSparkConfig.RSS_RESUBMIT_STAGE_WITH_FETCH_FAILURE_ENABLED;
import static org.apache.spark.shuffle.RssSparkConfig.RSS_SHUFFLE_MANAGER_GRPC_PORT;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrowsExactly;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyBoolean;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class RssShuffleManagerTest extends RssShuffleManagerTestBase {
  private static final String SPARK_ADAPTIVE_EXECUTION_ENABLED_KEY = "spark.sql.adaptive.enabled";

  @Test
  public void testGetDataDistributionType() {
    // case1
    SparkConf sparkConf = new SparkConf();
    sparkConf.set(SPARK_ADAPTIVE_EXECUTION_ENABLED_KEY, "true");
    assertEquals(
        ShuffleDataDistributionType.LOCAL_ORDER,
        RssShuffleManager.getDataDistributionType(sparkConf));

    // case2
    sparkConf = new SparkConf();
    sparkConf.set(SPARK_ADAPTIVE_EXECUTION_ENABLED_KEY, "false");
    assertEquals(
        RssClientConf.DATA_DISTRIBUTION_TYPE.defaultValue(),
        RssShuffleManager.getDataDistributionType(sparkConf));

    // case3
    sparkConf = new SparkConf();
    sparkConf.set(SPARK_ADAPTIVE_EXECUTION_ENABLED_KEY, "true");
    sparkConf.set(
        "spark." + RssClientConf.DATA_DISTRIBUTION_TYPE.key(),
        ShuffleDataDistributionType.NORMAL.name());
    assertEquals(
        ShuffleDataDistributionType.NORMAL, RssShuffleManager.getDataDistributionType(sparkConf));

    // case4
    sparkConf = new SparkConf();
    sparkConf.set(SPARK_ADAPTIVE_EXECUTION_ENABLED_KEY, "true");
    sparkConf.set(
        "spark." + RssClientConf.DATA_DISTRIBUTION_TYPE.key(),
        ShuffleDataDistributionType.LOCAL_ORDER.name());
    assertEquals(
        ShuffleDataDistributionType.LOCAL_ORDER,
        RssShuffleManager.getDataDistributionType(sparkConf));

    // case5
    sparkConf = new SparkConf();
    boolean aqeEnable = (boolean) sparkConf.get(SQLConf.ADAPTIVE_EXECUTION_ENABLED());
    if (aqeEnable) {
      assertEquals(
          ShuffleDataDistributionType.LOCAL_ORDER,
          RssShuffleManager.getDataDistributionType(sparkConf));
    } else {
      assertEquals(
          RssClientConf.DATA_DISTRIBUTION_TYPE.defaultValue(),
          RssShuffleManager.getDataDistributionType(sparkConf));
    }
  }

  @Test
  public void testGetRemoteStorageInfo() {
    setupMockedRssShuffleUtils(StatusCode.SUCCESS);

    SparkConf conf = new SparkConf();
    conf.set(RssSparkConfig.RSS_DYNAMIC_CLIENT_CONF_ENABLED.key(), "false");
    conf.set(RssSparkConfig.RSS_COORDINATOR_QUORUM.key(), "m1:8001,m2:8002");
    conf.set("spark.driver.host", "localhost");
    conf.set("spark.rss.storage.type", StorageType.LOCALFILE.name());
    conf.set(RssSparkConfig.RSS_TEST_MODE_ENABLE, true);

    // inject some hadoop configs
    conf.set("spark.rss.hadoop.k1", "v1");
    conf.set("spark.rss.hadoop.k2", "v2");

    RssShuffleManager shuffleManager = new RssShuffleManager(conf, true);
    RemoteStorageInfo remoteStorageInfo = shuffleManager.getRemoteStorageInfo();
    assertEquals(2, remoteStorageInfo.getConfItems().size());
  }

  @Test
  public void testCreateShuffleManagerServer() {
    setupMockedRssShuffleUtils(StatusCode.SUCCESS);

    SparkConf conf = new SparkConf();
    conf.set(RssSparkConfig.RSS_DYNAMIC_CLIENT_CONF_ENABLED.key(), "false");
    conf.set(RssSparkConfig.RSS_COORDINATOR_QUORUM.key(), "m1:8001,m2:8002");
    conf.set("spark.driver.host", "localhost");
    conf.set("spark.rss.storage.type", StorageType.LOCALFILE.name());
    conf.set(RssSparkConfig.RSS_TEST_MODE_ENABLE, true);
    // enable stage recompute
    conf.set("spark." + RssClientConfig.RSS_RESUBMIT_STAGE, "true");

    RssShuffleManager shuffleManager = new RssShuffleManager(conf, true);

    ConfigOption<Boolean> a = RSS_RESUBMIT_STAGE_WITH_FETCH_FAILURE_ENABLED;

    assertTrue(conf.get(RSS_SHUFFLE_MANAGER_GRPC_PORT) > 0);
  }

  @Test
  public void testRssShuffleManagerInterface() throws Exception {
    setupMockedRssShuffleUtils(StatusCode.SUCCESS);

    SparkConf conf = new SparkConf();
    conf.set(RssSparkConfig.RSS_DYNAMIC_CLIENT_CONF_ENABLED.key(), "false");
    conf.set(RssSparkConfig.RSS_COORDINATOR_QUORUM.key(), "m1:8001,m2:8002");
    conf.set("spark.rss.storage.type", StorageType.LOCALFILE.name());
    conf.set(RssSparkConfig.RSS_TEST_MODE_ENABLE, true);

    conf.set("spark.task.maxFailures", "3");
    RssShuffleManager shuffleManager = new RssShuffleManager(conf, true);
    assertEquals(shuffleManager.getMaxFetchFailures(), 2);
    // by default, the appId is null
    assertNull(shuffleManager.getAppId());
  }

  @ParameterizedTest
  @ValueSource(ints = {16, 20, 24})
  public void testRssShuffleManagerRegisterShuffle(int partitionIdBits) {
    BlockIdLayout layout =
        BlockIdLayout.from(
            63 - partitionIdBits - partitionIdBits - 2, partitionIdBits, partitionIdBits + 2);

    SparkConf conf = new SparkConf();
    conf.set(RssSparkConfig.RSS_DYNAMIC_CLIENT_CONF_ENABLED.key(), "false");
    conf.set(RssSparkConfig.RSS_COORDINATOR_QUORUM.key(), "m1:8001,m2:8002");
    conf.set("spark.rss.storage.type", StorageType.LOCALFILE.name());
    conf.set(RssSparkConfig.RSS_TEST_MODE_ENABLE, true);
    conf.set("spark.task.maxFailures", "4");

    conf.set(
        RssSparkConfig.SPARK_RSS_CONFIG_PREFIX + RssClientConf.BLOCKID_SEQUENCE_NO_BITS.key(),
        String.valueOf(layout.sequenceNoBits));
    conf.set(
        RssSparkConfig.SPARK_RSS_CONFIG_PREFIX + RssClientConf.BLOCKID_PARTITION_ID_BITS.key(),
        String.valueOf(layout.partitionIdBits));
    conf.set(
        RssSparkConfig.SPARK_RSS_CONFIG_PREFIX + RssClientConf.BLOCKID_TASK_ATTEMPT_ID_BITS.key(),
        String.valueOf(layout.taskAttemptIdBits));

    // register a shuffle with too many partitions should fail
    Partitioner mockPartitioner = mock(Partitioner.class);
    when(mockPartitioner.numPartitions()).thenReturn(layout.maxNumPartitions + 1);
    ShuffleDependency<String, String, String> mockDependency = mock(ShuffleDependency.class);
    when(mockDependency.partitioner()).thenReturn(mockPartitioner);

    RssShuffleManager shuffleManager = new RssShuffleManager(conf, true);

    RssException e =
        assertThrowsExactly(
            RssException.class, () -> shuffleManager.registerShuffle(0, mockDependency));
    assertEquals(
        "Cannot register shuffle with "
            + (layout.maxNumPartitions + 1)
            + " partitions because the configured block id layout supports at most "
            + layout.maxNumPartitions
            + " partitions.",
        e.getMessage());
  }

  @Test
  public void testWithStageRetry() {
    // case1: disable the stage retry
    SparkConf conf = createSparkConf();
    RssShuffleManager shuffleManager = new RssShuffleManager(conf, true);
    assertFalse(shuffleManager.isRssStageRetryEnabled());
    assertFalse(shuffleManager.isRssStageRetryForFetchFailureEnabled());
    assertFalse(shuffleManager.isRssStageRetryForWriteFailureEnabled());
    shuffleManager.stop();

    // case2: enable the stage retry
    conf.set(
        RssSparkConfig.SPARK_RSS_CONFIG_PREFIX + RssSparkConfig.RSS_RESUBMIT_STAGE_ENABLED.key(),
        "true");
    shuffleManager = new RssShuffleManager(conf, true);
    assertTrue(shuffleManager.isRssStageRetryEnabled());
    assertTrue(shuffleManager.isRssStageRetryForFetchFailureEnabled());
    assertTrue(shuffleManager.isRssStageRetryForWriteFailureEnabled());
    shuffleManager.stop();

    // case3: overwrite the stage retry
    conf.set(
        RssSparkConfig.SPARK_RSS_CONFIG_PREFIX
            + RSS_RESUBMIT_STAGE_WITH_FETCH_FAILURE_ENABLED.key(),
        "false");
    shuffleManager = new RssShuffleManager(conf, true);
    assertTrue(shuffleManager.isRssStageRetryEnabled());
    assertFalse(shuffleManager.isRssStageRetryForFetchFailureEnabled());
    assertTrue(shuffleManager.isRssStageRetryForWriteFailureEnabled());
    shuffleManager.stop();

    // case4: enable the partial stage retry of fetch failure
    conf = createSparkConf();
    conf.set(
        RssSparkConfig.SPARK_RSS_CONFIG_PREFIX
            + RSS_RESUBMIT_STAGE_WITH_FETCH_FAILURE_ENABLED.key(),
        "true");
    shuffleManager = new RssShuffleManager(conf, true);
    assertTrue(shuffleManager.isRssStageRetryEnabled());
    assertTrue(shuffleManager.isRssStageRetryForFetchFailureEnabled());
    assertFalse(shuffleManager.isRssStageRetryForWriteFailureEnabled());
    shuffleManager.stop();
  }

  private SparkConf createSparkConf() {
    SparkConf conf = new SparkConf();
    conf.set(RssSparkConfig.RSS_DYNAMIC_CLIENT_CONF_ENABLED.key(), "false");
    conf.set(RssSparkConfig.RSS_COORDINATOR_QUORUM.key(), "m1:8001,m2:8002");
    conf.set("spark.rss.storage.type", StorageType.LOCALFILE.name());
    conf.set(RssSparkConfig.RSS_TEST_MODE_ENABLE, true);
    conf.set("spark.task.maxFailures", "4");
    conf.set("spark.driver.host", "localhost");
    return conf;
  }

  @Test
  public void testReadCacheShuffleInfo() throws Exception {
    SparkConf conf = new SparkConf();
    conf.setAppName("testApp")
        .setMaster("local[2]")
        .set(RssSparkConfig.RSS_TEST_FLAG.key(), "true")
        .set(RssSparkConfig.RSS_TEST_MODE_ENABLE.key(), "true")
        .set(RssSparkConfig.RSS_CLIENT_SEND_CHECK_TIMEOUT_MS.key(), "10000")
        .set(RssSparkConfig.RSS_CLIENT_RETRY_MAX.key(), "10")
        .set(RssSparkConfig.RSS_CLIENT_SEND_CHECK_INTERVAL_MS.key(), "1000")
        .set(RssSparkConfig.RSS_STORAGE_TYPE.key(), StorageType.LOCALFILE.name())
        .set(RssSparkConfig.RSS_COORDINATOR_QUORUM.key(), "127.0.0.1:12345,127.0.0.1:12346");
    Map<String, Set<Long>> successBlocks = JavaUtils.newConcurrentMap();
    Map<String, FailedBlockSendTracker> taskToFailedBlockSendTracker = JavaUtils.newConcurrentMap();
    RssShuffleManager manager =
        spy(
            TestUtils.createShuffleManager(
                conf, false, null, successBlocks, taskToFailedBlockSendTracker));

    // case1: legal fetch and cache
    Supplier<ShuffleHandleInfo> func1 =
        () ->
            new SimpleShuffleHandleInfo(
                1, Collections.emptyMap(), RemoteStorageInfo.EMPTY_REMOTE_STORAGE);
    ShuffleHandleInfo handle1 = manager.getOrFetchShuffleHandle(1, func1);
    ShuffleHandleInfo handle2 = manager.getOrFetchShuffleHandle(1, func1);
    assertEquals(handle1, handle2);

    // case2: write-failure purge should evict cached read handle
    AtomicInteger fetchCount = new AtomicInteger();
    Supplier<ShuffleHandleInfo> func2 =
        () ->
            new SimpleShuffleHandleInfo(
                fetchCount.incrementAndGet(),
                Collections.emptyMap(),
                RemoteStorageInfo.EMPTY_REMOTE_STORAGE);
    ShuffleHandleInfo handle3 = manager.getOrFetchShuffleHandle(2, func2);
    ShuffleWriteClient shuffleWriteClient = mock(ShuffleWriteClient.class);
    manager.setAppId("app-123");
    setShuffleWriteClient(manager, shuffleWriteClient);
    setShuffleMetadata(manager, 2, 10, 20);
    manager.getBlockIdManager().add(2, 0, Collections.singletonList(1L));
    assertEquals(1, manager.getBlockIdManager().get(2, 0).getLongCardinality());
    manager.unregisterShuffleDataForWriteFailure(2);
    ShuffleHandleInfo retainedHandle = manager.getOrFetchShuffleHandle(2, func2);
    assertTrue(handle3 == retainedHandle);
    assertEquals(1, fetchCount.get());
    assertEquals(1, manager.getBlockIdManager().get(2, 0).getLongCardinality());
    manager.clearShuffleDataForWriteFailure(2);
    ShuffleHandleInfo handle4 = manager.getOrFetchShuffleHandle(2, func2);
    assertEquals(2, fetchCount.get());
    assertNotSame(handle3, handle4);
    assertEquals(10, manager.getPartitionNum(2));
    assertEquals(20, manager.getNumMaps(2));
    assertEquals(0, manager.getBlockIdManager().get(2, 0).getLongCardinality());
    verify(shuffleWriteClient, times(1)).unregisterShuffle("app-123", 2, 0);
    verify(manager, never()).unregisterShuffle(2);

    // case3: illegal fetch
    manager.clearShuffleHandleCache();
    Supplier<ShuffleHandleInfo> func3 = () -> null;
    try {
      ShuffleHandleInfo handle5 = manager.getOrFetchShuffleHandle(1, func3);
      fail();
    } catch (Exception e) {
      // ignore
    }
  }

  @Test
  public void testReadShuffleHandleCacheDisabledForWriteFailureRetry() {
    SparkConf conf = createSparkConf();
    conf.set(
        RssSparkConfig.SPARK_RSS_CONFIG_PREFIX
            + RssSparkConfig.RSS_READ_SHUFFLE_HANDLE_CACHE_ENABLED.key(),
        "true");
    RssShuffleManager manager = new RssShuffleManager(conf, true);
    assertTrue(manager.shouldCacheReadShuffleHandle());
    manager.stop();

    conf.set(
        RssSparkConfig.SPARK_RSS_CONFIG_PREFIX
            + RssSparkConfig.RSS_RESUBMIT_STAGE_WITH_WRITE_FAILURE_ENABLED.key(),
        "true");
    manager = new RssShuffleManager(conf, true);
    assertFalse(manager.shouldCacheReadShuffleHandle());
    manager.stop();
  }

  @Test
  public void testUnregisterShuffleDataForWriteFailureRequiresWriteClient() throws Exception {
    SparkConf conf = createSparkConf();
    RssShuffleManager manager = new RssShuffleManager(conf, true);
    manager.setAppId("app-123");
    AtomicInteger fetchCount = new AtomicInteger();
    Supplier<ShuffleHandleInfo> fetchHandle =
        () ->
            new SimpleShuffleHandleInfo(
                fetchCount.incrementAndGet(),
                Collections.emptyMap(),
                RemoteStorageInfo.EMPTY_REMOTE_STORAGE);
    ShuffleHandleInfo cachedHandle = manager.getOrFetchShuffleHandle(2, fetchHandle);
    manager.getBlockIdManager().add(2, 0, Collections.singletonList(1L));
    setShuffleWriteClient(manager, null);

    RssException e =
        assertThrowsExactly(RssException.class, () -> manager.unregisterShuffleDataForWriteFailure(2));
    assertTrue(e.getMessage().contains("appId=app-123"));
    assertTrue(e.getMessage().contains("shuffleId=2"));
    assertEquals(1, manager.getBlockIdManager().get(2, 0).getLongCardinality());
    assertTrue(cachedHandle == manager.getOrFetchShuffleHandle(2, fetchHandle));
    assertEquals(1, fetchCount.get());

    manager.stop();
  }

  @Test
  public void testWriteFailureMapOutputFailureRetainsLocalCaches() throws Exception {
    SparkConf conf = createSparkConf();
    Map<String, Set<Long>> successBlocks = JavaUtils.newConcurrentMap();
    Map<String, FailedBlockSendTracker> taskToFailedBlockSendTracker = JavaUtils.newConcurrentMap();
    RssShuffleManager manager =
        spy(
            TestUtils.createShuffleManager(
                conf, false, null, successBlocks, taskToFailedBlockSendTracker));
    manager.setAppId("app-123");
    doReturn(2).when(manager).getMaxFetchFailures();
    doThrow(new RuntimeException("map output cleanup failed"))
        .when(manager)
        .unregisterAllMapOutput(2);
    AtomicInteger fetchCount = new AtomicInteger();
    Supplier<ShuffleHandleInfo> fetchHandle =
        () ->
            new SimpleShuffleHandleInfo(
                fetchCount.incrementAndGet(),
                Collections.emptyMap(),
                RemoteStorageInfo.EMPTY_REMOTE_STORAGE);
    ShuffleHandleInfo cachedHandle = manager.getOrFetchShuffleHandle(2, fetchHandle);
    manager.getBlockIdManager().add(2, 0, Collections.singletonList(1L));
    ShuffleManagerGrpcService service = new ShuffleManagerGrpcService(manager);
    RssProtos.ReportShuffleWriteFailureRequest request =
        RssProtos.ReportShuffleWriteFailureRequest.newBuilder()
            .setAppId("app-123")
            .setShuffleId(2)
            .setStageAttemptId(1)
            .setStageAttemptNumber(0)
            .addShuffleServerIds(
                RssProtos.ShuffleServerId.newBuilder()
                    .setId("server-1")
                    .setIp("127.0.0.1")
                    .setPort(19999))
            .build();
    AtomicReference<RssProtos.ReportShuffleWriteFailureResponse> responseRef =
        new AtomicReference<>();
    StreamObserver<RssProtos.ReportShuffleWriteFailureResponse> observer =
        new StreamObserver<RssProtos.ReportShuffleWriteFailureResponse>() {
          @Override
          public void onNext(RssProtos.ReportShuffleWriteFailureResponse value) {
            responseRef.set(value);
          }

          @Override
          public void onError(Throwable t) {}

          @Override
          public void onCompleted() {}
        };

    for (int i = 0; i <= 2; i++) {
      service.reportShuffleWriteFailure(request, observer);
    }

    assertEquals(RssProtos.StatusCode.INTERNAL_ERROR, responseRef.get().getStatus());
    assertFalse(responseRef.get().getReSubmitWholeStage());
    assertEquals(1, manager.getBlockIdManager().get(2, 0).getLongCardinality());
    assertTrue(cachedHandle == manager.getOrFetchShuffleHandle(2, fetchHandle));
    assertEquals(1, fetchCount.get());
    verify(manager, never()).unregisterShuffleDataForWriteFailure(2, 0);
    verify(manager, never()).clearShuffleDataForWriteFailure(2);

    manager.stop();
  }

  @Test
  public void testGetReaderImplBypassesStaleCacheForWriteFailureRetry() throws Exception {
    SparkConf conf = createSparkConf();
    conf.set(
        RssSparkConfig.SPARK_RSS_CONFIG_PREFIX
            + RssSparkConfig.RSS_READ_SHUFFLE_HANDLE_CACHE_ENABLED.key(),
        "true");
    conf.set(
        RssSparkConfig.SPARK_RSS_CONFIG_PREFIX
            + RssSparkConfig.RSS_RESUBMIT_STAGE_WITH_WRITE_FAILURE_ENABLED.key(),
        "true");
    Map<String, Set<Long>> successBlocks = JavaUtils.newConcurrentMap();
    Map<String, FailedBlockSendTracker> taskToFailedBlockSendTracker = JavaUtils.newConcurrentMap();
    RssShuffleManager manager =
        spy(
            TestUtils.createShuffleManager(
                conf, false, null, successBlocks, taskToFailedBlockSendTracker));
    setRssStageRetryForWriteFailureEnabled(manager, true);
    assertFalse(manager.shouldCacheReadShuffleHandle());

    ShuffleServerInfo staleServer = new ShuffleServerInfo("stale", "127.0.0.1", 19998);
    ShuffleServerInfo freshServer = new ShuffleServerInfo("fresh", "127.0.0.1", 19999);
    ShuffleHandleInfo staleHandleInfo = createShuffleHandleInfo(2, staleServer);
    ShuffleHandleInfo freshHandleInfo = createShuffleHandleInfo(2, freshServer);
    manager.getOrFetchShuffleHandle(2, () -> staleHandleInfo);

    ShuffleWriteClient shuffleWriteClient = mock(ShuffleWriteClient.class);
    when(
            shuffleWriteClient.getShuffleResultForMultiPartV2(
                any(), any(), any(), anyInt(), any(), any()))
        .thenReturn(new ShuffleResult(Roaring64NavigableMap.bitmapOf(), null));
    setShuffleWriteClient(manager, shuffleWriteClient);
    doReturn(freshHandleInfo)
        .when(manager)
        .getShuffleHandleInfo(anyInt(), anyInt(), any(), anyBoolean());

    ShuffleDependency<String, String, String> dependency = mock(ShuffleDependency.class);
    Partitioner partitioner = mock(Partitioner.class);
    when(partitioner.numPartitions()).thenReturn(1);
    when(dependency.partitioner()).thenReturn(partitioner);
    when(dependency.serializer()).thenReturn(new KryoSerializer(conf));
    when(dependency.shuffleId()).thenReturn(2);
    when(dependency.aggregator()).thenReturn(Option.empty());
    when(dependency.keyOrdering()).thenReturn(Option.empty());
    when(dependency.mapSideCombine()).thenReturn(false);
    Broadcast<SimpleShuffleHandleInfo> broadcast = mock(Broadcast.class);
    when(broadcast.value()).thenReturn((SimpleShuffleHandleInfo) staleHandleInfo);
    RssShuffleHandle<String, String, String> rssShuffleHandle =
        new RssShuffleHandle<>(2, "app-123", 1, dependency, broadcast);
    TaskContext context = mock(TaskContext.class);
    when(context.stageId()).thenReturn(1);
    when(context.stageAttemptNumber()).thenReturn(1);
    when(context.taskAttemptId()).thenReturn(1L);
    when(context.attemptNumber()).thenReturn(0);
    when(context.taskMetrics()).thenReturn(new TaskMetrics());

    manager.getReaderImpl(
        rssShuffleHandle,
        0,
        Integer.MAX_VALUE,
        0,
        1,
        context,
        null,
        Roaring64NavigableMap.bitmapOf(0),
        0,
        0);

    @SuppressWarnings("unchecked")
    ArgumentCaptor<Map<ShuffleServerInfo, Set<Integer>>> serverToPartitionsCaptor =
        ArgumentCaptor.forClass(Map.class);
    verify(shuffleWriteClient, times(1))
        .getShuffleResultForMultiPartV2(
            any(),
            serverToPartitionsCaptor.capture(),
            eq("app-123"),
            eq(2),
            any(),
            any());
    verify(manager, times(1)).getShuffleHandleInfo(1, 1, rssShuffleHandle, false);
    assertTrue(serverToPartitionsCaptor.getValue().containsKey(freshServer));
    assertFalse(serverToPartitionsCaptor.getValue().containsKey(staleServer));
  }

  private static ShuffleHandleInfo createShuffleHandleInfo(
      int shuffleId, ShuffleServerInfo shuffleServerInfo) {
    Map<Integer, List<ShuffleServerInfo>> partitionToServers = JavaUtils.newConcurrentMap();
    partitionToServers.put(0, Collections.singletonList(shuffleServerInfo));
    return new SimpleShuffleHandleInfo(
        shuffleId, partitionToServers, RemoteStorageInfo.EMPTY_REMOTE_STORAGE);
  }

  @SuppressWarnings("unchecked")
  private static void setShuffleMetadata(
      RssShuffleManager manager, int shuffleId, int partitionNum, int numMaps) throws Exception {
    Field partitionNumField =
        RssShuffleManagerBase.class.getDeclaredField("shuffleIdToPartitionNum");
    partitionNumField.setAccessible(true);
    Map<Integer, Integer> partitionNums = (Map<Integer, Integer>) partitionNumField.get(manager);
    if (partitionNums == null) {
      partitionNums = JavaUtils.newConcurrentMap();
      partitionNumField.set(manager, partitionNums);
    }
    partitionNums.put(shuffleId, partitionNum);

    Field numMapsField = RssShuffleManagerBase.class.getDeclaredField("shuffleIdToNumMapTasks");
    numMapsField.setAccessible(true);
    Map<Integer, Integer> numMapTasks = (Map<Integer, Integer>) numMapsField.get(manager);
    if (numMapTasks == null) {
      numMapTasks = JavaUtils.newConcurrentMap();
      numMapsField.set(manager, numMapTasks);
    }
    numMapTasks.put(shuffleId, numMaps);
  }

  private static void setShuffleWriteClient(
      RssShuffleManager manager, ShuffleWriteClient shuffleWriteClient) throws Exception {
    Field shuffleWriteClientField =
        RssShuffleManagerBase.class.getDeclaredField("shuffleWriteClient");
    shuffleWriteClientField.setAccessible(true);
    shuffleWriteClientField.set(manager, shuffleWriteClient);
  }

  private static void setRssStageRetryForWriteFailureEnabled(
      RssShuffleManager manager, boolean enabled) throws Exception {
    Field stageRetryField =
        RssShuffleManagerBase.class.getDeclaredField("rssStageRetryForWriteFailureEnabled");
    stageRetryField.setAccessible(true);
    stageRetryField.set(manager, enabled);
  }
}
