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

package org.apache.uniffle.server;

import java.io.File;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.protobuf.ByteString;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.RangeMap;
import com.google.common.collect.Sets;
import io.grpc.stub.StreamObserver;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.roaringbitmap.longlong.Roaring64NavigableMap;

import org.apache.uniffle.common.BufferSegment;
import org.apache.uniffle.common.PartitionRange;
import org.apache.uniffle.common.RemoteStorageInfo;
import org.apache.uniffle.common.ShuffleDataResult;
import org.apache.uniffle.common.ShufflePartitionedBlock;
import org.apache.uniffle.common.ShufflePartitionedData;
import org.apache.uniffle.common.config.RssBaseConf;
import org.apache.uniffle.common.exception.InvalidRequestException;
import org.apache.uniffle.common.exception.NoBufferForHugePartitionException;
import org.apache.uniffle.common.exception.NoRegisterException;
import org.apache.uniffle.common.exception.RssException;
import org.apache.uniffle.common.rpc.StatusCode;
import org.apache.uniffle.common.util.BlockIdLayout;
import org.apache.uniffle.common.util.ChecksumUtils;
import org.apache.uniffle.common.util.RssUtils;
import org.apache.uniffle.proto.RssProtos;
import org.apache.uniffle.server.block.DefaultShuffleBlockIdManager;
import org.apache.uniffle.server.buffer.ShuffleBufferType;
import org.apache.uniffle.server.buffer.PreAllocatedBufferInfo;
import org.apache.uniffle.server.buffer.ShuffleBuffer;
import org.apache.uniffle.server.buffer.ShuffleBufferManager;
import org.apache.uniffle.server.event.PurgeEvent;
import org.apache.uniffle.server.storage.LocalStorageManager;
import org.apache.uniffle.server.storage.StorageManager;
import org.apache.uniffle.storage.HadoopTestBase;
import org.apache.uniffle.storage.common.LocalStorage;
import org.apache.uniffle.storage.handler.impl.HadoopClientReadHandler;
import org.apache.uniffle.storage.util.ShuffleStorageUtils;
import org.apache.uniffle.storage.util.StorageType;

import static org.apache.uniffle.common.StorageType.MEMORY_LOCALFILE;
import static org.apache.uniffle.server.ShuffleServerConf.CLIENT_MAX_CONCURRENCY_LIMITATION_OF_ONE_PARTITION;
import static org.apache.uniffle.server.ShuffleServerConf.SERVER_MAX_CONCURRENCY_OF_ONE_PARTITION;
import static org.apache.uniffle.server.merge.ShuffleMergeManager.MERGE_APP_SUFFIX;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class ShuffleTaskManagerTest extends HadoopTestBase {

  private static final AtomicInteger ATOMIC_INT = new AtomicInteger(0);

  private ShuffleServer shuffleServer;

  @TempDir File tempDir1;
  @TempDir File tempDir2;

  private static class CapturingStreamObserver<T> implements StreamObserver<T> {
    private T value;
    private boolean completed;
    private Throwable error;

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

  @BeforeEach
  public void beforeEach() {
    ShuffleServerMetrics.clear();
    ShuffleServerMetrics.register();
    assertTrue(this.tempDir1.isDirectory());
    assertTrue(this.tempDir2.isDirectory());
  }

  @AfterEach
  public void afterEach() throws Exception {
    if (shuffleServer != null) {
      shuffleServer.stopServer();
      shuffleServer = null;
    }
    ShuffleServerMetrics.clear();
  }

  private ShuffleServerConf constructServerConfWithLocalfile() {
    String confFile = ClassLoader.getSystemResource("server.conf").getFile();
    ShuffleServerConf conf = new ShuffleServerConf(confFile);
    conf.set(ShuffleServerConf.RPC_SERVER_PORT, 1234);
    conf.set(ShuffleServerConf.RSS_COORDINATOR_QUORUM, "localhost:9527");
    conf.set(ShuffleServerConf.JETTY_HTTP_PORT, 12345);
    conf.set(ShuffleServerConf.JETTY_CORE_POOL_SIZE, 64);
    conf.set(ShuffleServerConf.SERVER_BUFFER_CAPACITY, 1000L);
    conf.set(ShuffleServerConf.SERVER_MEMORY_SHUFFLE_HIGHWATERMARK_PERCENTAGE, 50.0);
    conf.set(ShuffleServerConf.SERVER_MEMORY_SHUFFLE_LOWWATERMARK_PERCENTAGE, 0.0);
    conf.set(ShuffleServerConf.SERVER_COMMIT_TIMEOUT, 10000L);
    conf.set(ShuffleServerConf.SERVER_APP_EXPIRED_WITHOUT_HEARTBEAT, 100000L);
    conf.set(ShuffleServerConf.HEALTH_CHECK_ENABLE, false);

    conf.setString(ShuffleServerConf.RSS_STORAGE_TYPE.key(), StorageType.LOCALFILE.name());
    conf.set(ShuffleServerConf.RSS_TEST_MODE_ENABLE, true);
    conf.setString(
        ShuffleServerConf.RSS_STORAGE_BASE_PATH.key(),
        tempDir1.getAbsolutePath() + "," + tempDir2.getAbsolutePath());
    return conf;
  }

  /** Test the shuffleMeta's diskSize when app is removed. */
  @Test
  public void appPurgeWithLocalfileTest() throws Exception {
    ShuffleServerConf conf = constructServerConfWithLocalfile();
    shuffleServer = new ShuffleServer(conf);
    ShuffleTaskManager shuffleTaskManager = shuffleServer.getShuffleTaskManager();

    String appId = "removeShuffleDataWithLocalfileTest";

    int shuffleNum = 4;
    for (int i = 0; i < shuffleNum; i++) {
      shuffleTaskManager.registerShuffle(
          appId,
          i,
          Lists.newArrayList(new PartitionRange(0, 1)),
          RemoteStorageInfo.EMPTY_REMOTE_STORAGE,
          StringUtils.EMPTY);

      ShufflePartitionedData partitionedData0 = createPartitionedData(1, 1, 35);
      shuffleTaskManager.requireBuffer(35);
      shuffleTaskManager.cacheShuffleData(appId, i, false, partitionedData0);
      shuffleTaskManager.updateCachedBlockIds(appId, i, partitionedData0);
    }

    assertEquals(1, shuffleTaskManager.getAppIds().size());
    for (int i = 0; i < shuffleNum; i++) {
      shuffleTaskManager.commitShuffle(appId, i);
    }

    shuffleTaskManager.removeResources(appId, false);
    for (String path : conf.get(ShuffleServerConf.RSS_STORAGE_BASE_PATH)) {
      String appPath = path + "/" + appId;
      assertFalse(new File(appPath).exists());
    }

    // once the app is removed. the disk size should be 0
    LocalStorageManager localStorageManager =
        (LocalStorageManager) shuffleServer.getStorageManager();
    for (LocalStorage localStorage : localStorageManager.getStorages()) {
      assertEquals(0, localStorage.getMetaData().getDiskSize().get());
    }
  }

  @Test
  public void hugePartitionMemoryUsageLimitTest() throws Exception {
    String confFile = ClassLoader.getSystemResource("server.conf").getFile();
    ShuffleServerConf conf = new ShuffleServerConf(confFile);
    conf.setString(ShuffleServerConf.RSS_STORAGE_TYPE.key(), StorageType.MEMORY_LOCALFILE.name());
    conf.setString(ShuffleServerConf.HUGE_PARTITION_SIZE_THRESHOLD.key(), "1K");
    conf.setString("rss.server.buffer.capacity", "10K");
    conf.set(ShuffleServerConf.HUGE_PARTITION_MEMORY_USAGE_LIMITATION_RATIO, 0.1);

    shuffleServer = new ShuffleServer(conf);
    ShuffleTaskManager shuffleTaskManager = shuffleServer.getShuffleTaskManager();

    String appId = "hugePartitionMemoryUsageLimitTest_appId";
    int shuffleId = 1;

    // case1, expect NoRegisterException
    try {
      shuffleTaskManager.requireBuffer(appId, 1, Arrays.asList(1), Arrays.asList(500), 500);
      fail("Should thow NoRegisterException");
    } catch (Exception e) {
      assertTrue(e instanceof NoRegisterException);
    }

    shuffleTaskManager.registerShuffle(
        appId,
        shuffleId,
        Lists.newArrayList(new PartitionRange(1, 1)),
        RemoteStorageInfo.EMPTY_REMOTE_STORAGE,
        StringUtils.EMPTY);

    // case2
    try {
      long requiredId =
          shuffleTaskManager.requireBuffer(appId, 1, Arrays.asList(1), Arrays.asList(500), 500);
      assertNotEquals(-1, requiredId);
    } catch (Exception e) {
      fail("Should not throw Exception");
    }

    // case3
    ShufflePartitionedData partitionedData0 = createPartitionedData(1, 1, 500);
    shuffleTaskManager.cacheShuffleData(appId, shuffleId, true, partitionedData0);
    shuffleTaskManager.updateCachedBlockIds(appId, shuffleId, 1, partitionedData0);
    try {
      long requiredId =
          shuffleTaskManager.requireBuffer(appId, 1, Arrays.asList(1), Arrays.asList(500), 500);
      assertNotEquals(-1, requiredId);
    } catch (Exception e) {
      fail("Should not throw Exception");
    }

    // case4
    partitionedData0 = createPartitionedData(1, 1, 500);
    shuffleTaskManager.cacheShuffleData(appId, shuffleId, true, partitionedData0);
    shuffleTaskManager.updateCachedBlockIds(appId, shuffleId, 1, partitionedData0);
    try {
      shuffleTaskManager.requireBuffer(appId, 1, Arrays.asList(1), Arrays.asList(500), 500);
      fail("Should throw NoBufferForHugePartitionException");
    } catch (Exception e) {
      assertTrue(e instanceof NoBufferForHugePartitionException);
    }

    // test: to simulate the flush delay, it also will limit the huge partition if the partial data
    // is in flush queue.
    ShuffleFlushManager shuffleFlushManager = shuffleServer.getShuffleFlushManager();
    ShuffleFlushManager spyFlushManager = spy(shuffleFlushManager);
    doAnswer(
            invocationOnMock -> {
              Thread.sleep(10000);
              return invocationOnMock.callRealMethod();
            })
        .when(spyFlushManager)
        .addToFlushQueue(any(ShuffleDataFlushEvent.class));
    shuffleTaskManager.setShuffleFlushManager(spyFlushManager);
    shuffleServer.getShuffleBufferManager().setBufferFlushThreshold(1024);
    partitionedData0 = createPartitionedData(1, 1, 500);
    shuffleTaskManager.cacheShuffleData(appId, shuffleId, true, partitionedData0);
    shuffleTaskManager.updateCachedBlockIds(appId, shuffleId, 1, partitionedData0);
    try {
      shuffleTaskManager.requireBuffer(appId, 1, Arrays.asList(1), Arrays.asList(500), 500);
      fail("Should throw NoBufferForHugePartitionException");
    } catch (Exception e) {
      assertTrue(e instanceof NoBufferForHugePartitionException);
    }

    // metrics test
    assertEquals(0, ShuffleServerMetrics.counterTotalRequireBufferFailedForHugePartition.get());
    assertEquals(0, ShuffleServerMetrics.counterTotalRequireBufferFailedForRegularPartition.get());
    assertEquals(1, ShuffleServerMetrics.counterTotalAppWithHugePartitionNum.get());
    assertEquals(1, ShuffleServerMetrics.counterTotalHugePartitionNum.get());
    assertEquals(1, ShuffleServerMetrics.gaugeHugePartitionNum.get());
    assertEquals(1, ShuffleServerMetrics.gaugeAppWithHugePartitionNum.get());

    // case5
    shuffleTaskManager.removeResources(appId, false);
    assertEquals(0, ShuffleServerMetrics.gaugeHugePartitionNum.get());
    assertEquals(0, ShuffleServerMetrics.gaugeAppWithHugePartitionNum.get());
  }

  @Test
  public void partitionDataSizeSummaryTest() throws Exception {
    String confFile = ClassLoader.getSystemResource("server.conf").getFile();
    ShuffleServerConf conf = new ShuffleServerConf(confFile);
    conf.setString(ShuffleServerConf.RSS_STORAGE_TYPE.key(), StorageType.MEMORY_LOCALFILE.name());
    shuffleServer = new ShuffleServer(conf);
    ShuffleTaskManager shuffleTaskManager = shuffleServer.getShuffleTaskManager();

    String appId = "partitionDataSizeSummaryTest";
    int shuffleId = 1;

    shuffleTaskManager.registerShuffle(
        appId,
        shuffleId,
        Lists.newArrayList(new PartitionRange(0, 1)),
        RemoteStorageInfo.EMPTY_REMOTE_STORAGE,
        StringUtils.EMPTY);

    // case1
    ShufflePartitionedData partitionedData0 = createPartitionedData(1, 1, 35);
    long size1 = partitionedData0.getTotalBlockEncodedLength();
    shuffleTaskManager.updateCachedBlockIds(appId, shuffleId, 1, partitionedData0);

    assertEquals(size1, shuffleTaskManager.getShuffleTaskInfo(appId).getTotalDataSize());

    // case2
    partitionedData0 = createPartitionedData(1, 1, 35);
    long size2 = partitionedData0.getTotalBlockEncodedLength();
    shuffleTaskManager.updateCachedBlockIds(appId, shuffleId, 1, partitionedData0);
    assertEquals(size1 + size2, shuffleTaskManager.getShuffleTaskInfo(appId).getTotalDataSize());
    assertEquals(
        size1 + size2, shuffleTaskManager.getShuffleTaskInfo(appId).getPartitionDataSize(1, 1));
  }

  @Test
  public void registerShuffleTest() throws Exception {
    String confFile = ClassLoader.getSystemResource("server.conf").getFile();
    ShuffleServerConf conf = new ShuffleServerConf(confFile);
    conf.set(ShuffleServerConf.SERVER_BUFFER_CAPACITY, 128L);
    conf.set(ShuffleServerConf.SERVER_MEMORY_SHUFFLE_HIGHWATERMARK_PERCENTAGE, 50.0);
    conf.set(ShuffleServerConf.SERVER_MEMORY_SHUFFLE_LOWWATERMARK_PERCENTAGE, 0.0);
    conf.setString(ShuffleServerConf.RSS_STORAGE_TYPE.key(), StorageType.HDFS.name());
    conf.set(ShuffleServerConf.RSS_TEST_MODE_ENABLE, true);
    conf.set(ShuffleServerConf.SERVER_COMMIT_TIMEOUT, 10000L);
    conf.set(ShuffleServerConf.HEALTH_CHECK_ENABLE, false);
    shuffleServer = new ShuffleServer(conf);
    ShuffleTaskManager shuffleTaskManager = shuffleServer.getShuffleTaskManager();

    String appId = "registerTest1";
    int shuffleId = 1;

    shuffleTaskManager.registerShuffle(
        appId,
        shuffleId,
        Lists.newArrayList(new PartitionRange(0, 1)),
        RemoteStorageInfo.EMPTY_REMOTE_STORAGE,
        StringUtils.EMPTY);
    shuffleTaskManager.registerShuffle(
        appId,
        shuffleId,
        Lists.newArrayList(new PartitionRange(2, 3)),
        RemoteStorageInfo.EMPTY_REMOTE_STORAGE,
        StringUtils.EMPTY);

    Map<String, Map<Integer, RangeMap<Integer, ShuffleBuffer>>> bufferPool =
        shuffleServer.getShuffleBufferManager().getBufferPool();

    assertNotNull(bufferPool.get(appId).get(shuffleId).get(0));
    ShuffleBuffer buffer = bufferPool.get(appId).get(shuffleId).get(0);
    assertEquals(buffer, bufferPool.get(appId).get(shuffleId).get(1));
    assertNotNull(bufferPool.get(appId).get(shuffleId).get(2));
    assertEquals(
        bufferPool.get(appId).get(shuffleId).get(2), bufferPool.get(appId).get(shuffleId).get(3));

    // register again
    shuffleTaskManager.registerShuffle(
        appId,
        shuffleId,
        Lists.newArrayList(new PartitionRange(0, 1)),
        RemoteStorageInfo.EMPTY_REMOTE_STORAGE,
        StringUtils.EMPTY);
    assertEquals(buffer, bufferPool.get(appId).get(shuffleId).get(0));
  }

  @Test
  public void writeProcessTest() throws Exception {
    String confFile = ClassLoader.getSystemResource("server.conf").getFile();
    ShuffleServerConf conf = new ShuffleServerConf(confFile);
    final String remoteStorage = HDFS_URI + "rss/test";
    final String appId = "testAppId";
    final int shuffleId = 1;
    conf.set(ShuffleServerConf.SERVER_BUFFER_CAPACITY, 128L);
    conf.set(ShuffleServerConf.SERVER_MEMORY_SHUFFLE_HIGHWATERMARK_PERCENTAGE, 50.0);
    conf.set(ShuffleServerConf.SERVER_MEMORY_SHUFFLE_LOWWATERMARK_PERCENTAGE, 0.0);
    conf.setString(ShuffleServerConf.RSS_STORAGE_TYPE.key(), StorageType.HDFS.name());
    conf.set(ShuffleServerConf.RSS_TEST_MODE_ENABLE, true);
    conf.set(ShuffleServerConf.SERVER_COMMIT_TIMEOUT, 10000L);
    conf.set(ShuffleServerConf.SERVER_PRE_ALLOCATION_EXPIRED, 3000L);
    conf.set(ShuffleServerConf.HEALTH_CHECK_ENABLE, false);
    shuffleServer = new ShuffleServer(conf);
    ShuffleTaskManager shuffleTaskManager = shuffleServer.getShuffleTaskManager();
    shuffleTaskManager.registerShuffle(
        appId,
        shuffleId,
        Lists.newArrayList(new PartitionRange(1, 1)),
        new RemoteStorageInfo(remoteStorage),
        StringUtils.EMPTY);
    shuffleTaskManager.registerShuffle(
        appId,
        shuffleId,
        Lists.newArrayList(new PartitionRange(2, 2)),
        new RemoteStorageInfo(remoteStorage),
        StringUtils.EMPTY);
    final List<ShufflePartitionedBlock> expectedBlocks1 = Lists.newArrayList();
    final List<ShufflePartitionedBlock> expectedBlocks2 = Lists.newArrayList();
    final Map<Long, PreAllocatedBufferInfo> bufferIds = shuffleTaskManager.getRequireBufferIds();

    shuffleTaskManager.requireBuffer(10);
    shuffleTaskManager.requireBuffer(10);
    shuffleTaskManager.requireBuffer(10);
    assertEquals(3, bufferIds.size());
    // required buffer should be clear if it doesn't receive data after timeout
    Thread.sleep(6000);
    assertEquals(0, bufferIds.size());

    shuffleTaskManager.commitShuffle(appId, shuffleId);

    // won't flush for partition 1-1
    ShufflePartitionedData partitionedData0 = createPartitionedData(1, 1, 35);
    expectedBlocks1.addAll(Lists.newArrayList(partitionedData0.getBlockList()));
    long bufferId = shuffleTaskManager.requireBuffer(35);
    assertEquals(1, bufferIds.size());
    PreAllocatedBufferInfo pabi = bufferIds.get(bufferId);
    assertEquals(35, pabi.getRequireSize());
    StatusCode sc = shuffleTaskManager.cacheShuffleData(appId, shuffleId, true, partitionedData0);
    shuffleTaskManager.updateCachedBlockIds(appId, shuffleId, partitionedData0);
    // the required id won't be removed in shuffleTaskManager, it is removed in Grpc service
    assertEquals(1, bufferIds.size());
    assertEquals(StatusCode.SUCCESS, sc);
    shuffleTaskManager.commitShuffle(appId, shuffleId);
    // manually release the pre allocate buffer
    shuffleTaskManager.removeAndReleasePreAllocatedBuffer(bufferId);

    ShuffleFlushManager shuffleFlushManager = shuffleServer.getShuffleFlushManager();
    assertEquals(1, shuffleFlushManager.getCommittedBlockCount(appId, shuffleId));

    // flush for partition 1-1
    ShufflePartitionedData partitionedData1 = createPartitionedData(1, 2, 35);
    expectedBlocks1.addAll(Lists.newArrayList(partitionedData1.getBlockList()));
    bufferId = shuffleTaskManager.requireBuffer(70);
    sc = shuffleTaskManager.cacheShuffleData(appId, shuffleId, true, partitionedData1);
    shuffleTaskManager.updateCachedBlockIds(appId, shuffleId, partitionedData1);
    assertEquals(StatusCode.SUCCESS, sc);
    shuffleTaskManager.removeAndReleasePreAllocatedBuffer(bufferId);
    waitForFlush(shuffleFlushManager, appId, shuffleId, 2 + 1);

    // won't flush for partition 1-1
    ShufflePartitionedData partitionedData2 = createPartitionedData(1, 1, 30);
    expectedBlocks1.addAll(Lists.newArrayList(partitionedData2.getBlockList()));
    // receive un-preAllocation data
    sc = shuffleTaskManager.cacheShuffleData(appId, shuffleId, false, partitionedData2);
    shuffleTaskManager.updateCachedBlockIds(appId, shuffleId, partitionedData2);
    assertEquals(StatusCode.SUCCESS, sc);

    // won't flush for partition 2-2
    ShufflePartitionedData partitionedData3 = createPartitionedData(2, 1, 30);
    expectedBlocks2.addAll(Lists.newArrayList(partitionedData3.getBlockList()));
    bufferId = shuffleTaskManager.requireBuffer(30);
    sc = shuffleTaskManager.cacheShuffleData(appId, shuffleId, true, partitionedData3);
    shuffleTaskManager.updateCachedBlockIds(appId, shuffleId, partitionedData3);
    shuffleTaskManager.removeAndReleasePreAllocatedBuffer(bufferId);
    assertEquals(StatusCode.SUCCESS, sc);

    // flush for partition 2-2
    ShufflePartitionedData partitionedData4 = createPartitionedData(2, 1, 35);
    expectedBlocks2.addAll(Lists.newArrayList(partitionedData4.getBlockList()));
    bufferId = shuffleTaskManager.requireBuffer(35);
    sc = shuffleTaskManager.cacheShuffleData(appId, shuffleId, true, partitionedData4);
    shuffleTaskManager.updateCachedBlockIds(appId, shuffleId, partitionedData4);
    shuffleTaskManager.removeAndReleasePreAllocatedBuffer(bufferId);
    assertEquals(StatusCode.SUCCESS, sc);

    shuffleTaskManager.commitShuffle(appId, shuffleId);
    // 3 new blocks should be committed
    waitForFlush(shuffleFlushManager, appId, shuffleId, 2 + 1 + 3);

    // flush for partition 1-1
    ShufflePartitionedData partitionedData5 = createPartitionedData(1, 2, 35);
    shuffleTaskManager.updateCachedBlockIds(appId, shuffleId, partitionedData5);
    expectedBlocks1.addAll(Lists.newArrayList(partitionedData5.getBlockList()));
    bufferId = shuffleTaskManager.requireBuffer(70);
    sc = shuffleTaskManager.cacheShuffleData(appId, shuffleId, true, partitionedData5);
    assertEquals(StatusCode.SUCCESS, sc);
    shuffleTaskManager.removeAndReleasePreAllocatedBuffer(bufferId);

    // 2 new blocks should be committed
    waitForFlush(shuffleFlushManager, appId, shuffleId, 2 + 1 + 3 + 2);
    shuffleTaskManager.commitShuffle(appId, shuffleId);
    shuffleTaskManager.commitShuffle(appId, shuffleId);

    validate(appId, shuffleId, 1, expectedBlocks1, remoteStorage);
    validate(appId, shuffleId, 2, expectedBlocks2, remoteStorage);

    // flush for partition 0-1
    ShufflePartitionedData partitionedData7 = createPartitionedData(1, 2, 35);
    shuffleTaskManager.updateCachedBlockIds(appId, shuffleId, partitionedData7);
    bufferId = shuffleTaskManager.requireBuffer(70);
    sc = shuffleTaskManager.cacheShuffleData(appId, shuffleId, true, partitionedData7);
    assertEquals(StatusCode.SUCCESS, sc);
    shuffleTaskManager.removeAndReleasePreAllocatedBuffer(bufferId);

    // 2 new blocks should be committed
    waitForFlush(shuffleFlushManager, appId, shuffleId, 2 + 1 + 3 + 2 + 2);
    shuffleFlushManager.removeResources(appId);
    try {
      shuffleTaskManager.commitShuffle(appId, shuffleId);
      fail("Exception should be thrown");
    } catch (Exception e) {
      assertTrue(e.getMessage().startsWith("Shuffle data commit timeout for"));
    }
  }

  /**
   * Clean up the shuffle data of stage level for one app
   *
   * @throws Exception
   */
  @Test
  public void removeShuffleDataWithHdfsTest() throws Exception {
    String confFile = ClassLoader.getSystemResource("server.conf").getFile();
    ShuffleServerConf conf = new ShuffleServerConf(confFile);
    String storageBasePath = HDFS_URI + "rss/removeShuffleDataWithHdfsTest";
    conf.set(ShuffleServerConf.RSS_TEST_MODE_ENABLE, true);
    conf.set(ShuffleServerConf.RPC_SERVER_PORT, 1234);
    conf.set(ShuffleServerConf.RSS_COORDINATOR_QUORUM, "localhost:9527");
    conf.set(ShuffleServerConf.JETTY_HTTP_PORT, 12345);
    conf.set(ShuffleServerConf.JETTY_CORE_POOL_SIZE, 64);
    conf.set(ShuffleServerConf.SERVER_BUFFER_CAPACITY, 128L);
    conf.set(ShuffleServerConf.SERVER_MEMORY_SHUFFLE_HIGHWATERMARK_PERCENTAGE, 50.0);
    conf.set(ShuffleServerConf.SERVER_MEMORY_SHUFFLE_LOWWATERMARK_PERCENTAGE, 0.0);
    conf.set(ShuffleServerConf.SERVER_COMMIT_TIMEOUT, 10000L);
    conf.set(ShuffleServerConf.SERVER_APP_EXPIRED_WITHOUT_HEARTBEAT, 2000L);
    conf.set(ShuffleServerConf.HEALTH_CHECK_ENABLE, false);

    shuffleServer = new ShuffleServer(conf);

    ShuffleTaskManager shuffleTaskManager = shuffleServer.getShuffleTaskManager();

    String appId = "removeShuffleDataTest1";
    for (int i = 0; i < 4; i++) {
      shuffleTaskManager.registerShuffle(
          appId,
          i,
          Lists.newArrayList(new PartitionRange(0, 1)),
          new RemoteStorageInfo(storageBasePath, Maps.newHashMap()),
          StringUtils.EMPTY);
    }
    shuffleTaskManager.refreshAppId(appId);

    assertEquals(1, shuffleTaskManager.getAppIds().size());

    ShufflePartitionedData partitionedData0 = createPartitionedData(1, 1, 35);

    shuffleTaskManager.requireBuffer(35);
    shuffleTaskManager.requireBuffer(35);
    shuffleTaskManager.cacheShuffleData(appId, 0, false, partitionedData0);
    shuffleTaskManager.updateCachedBlockIds(appId, 0, partitionedData0);
    shuffleTaskManager.cacheShuffleData(appId, 1, false, partitionedData0);
    shuffleTaskManager.updateCachedBlockIds(appId, 1, partitionedData0);
    shuffleTaskManager.refreshAppId(appId);
    shuffleTaskManager.checkResourceStatus();

    assertEquals(1, shuffleTaskManager.getAppIds().size());

    ShuffleBufferManager shuffleBufferManager = shuffleServer.getShuffleBufferManager();
    RangeMap<Integer, ShuffleBuffer> rangeMap =
        shuffleBufferManager.getBufferPool().get(appId).get(0);
    assertFalse(rangeMap.asMapOfRanges().isEmpty());
    shuffleTaskManager.commitShuffle(appId, 0);

    // Before removing shuffle resources
    String appBasePath = ShuffleStorageUtils.getFullShuffleDataFolder(storageBasePath, appId);
    String shufflePath0 = ShuffleStorageUtils.getFullShuffleDataFolder(appBasePath, "0");
    assertTrue(fs.exists(new Path(shufflePath0)));

    // After removing the shuffle id of 0 resources
    shuffleTaskManager.removeShuffleDataSync(appId, 0);
    assertFalse(fs.exists(new Path(shufflePath0)));
    assertTrue(fs.exists(new Path(appBasePath)));
    assertNull(shuffleBufferManager.getBufferPool().get(appId).get(0));
    assertNotNull(shuffleBufferManager.getBufferPool().get(appId).get(1));

    // the shufflePurgeEvent only will delete the children folders
    // Once the app is expired, all the app folders should be deleted.
    shuffleTaskManager.removeResources(appId, false);
    assertFalse(fs.exists(new Path(appBasePath)));
  }

  @Test
  public void removeShuffleDataWithLocalfileTest() throws Exception {
    ShuffleServerConf conf = constructServerConfWithLocalfile();
    shuffleServer = new ShuffleServer(conf);
    ShuffleTaskManager shuffleTaskManager = shuffleServer.getShuffleTaskManager();

    String appId = "removeShuffleDataWithLocalfileTest";

    int shuffleNum = 4;
    for (int i = 0; i < shuffleNum; i++) {
      shuffleTaskManager.registerShuffle(
          appId,
          i,
          Lists.newArrayList(new PartitionRange(0, 1)),
          RemoteStorageInfo.EMPTY_REMOTE_STORAGE,
          StringUtils.EMPTY);

      ShufflePartitionedData partitionedData0 = createPartitionedData(1, 1, 35);
      shuffleTaskManager.requireBuffer(35);
      shuffleTaskManager.cacheShuffleData(appId, i, false, partitionedData0);
      shuffleTaskManager.updateCachedBlockIds(appId, i, partitionedData0);
    }

    assertEquals(1, shuffleTaskManager.getAppIds().size());

    for (int i = 0; i < shuffleNum; i++) {
      shuffleTaskManager.commitShuffle(appId, i);
      shuffleTaskManager.removeShuffleDataSync(appId, i);
    }

    for (String path : conf.get(ShuffleServerConf.RSS_STORAGE_BASE_PATH)) {
      String appPath = path + "/" + appId;
      File[] files = new File(appPath).listFiles();
      if (files != null) {
        assertEquals(0, files.length);
      }
    }

    // the shufflePurgeEvent only will delete the children folders
    // Once the app is expired, all the app folders should be deleted.
    shuffleTaskManager.removeResources(appId, false);
    for (String path : conf.get(ShuffleServerConf.RSS_STORAGE_BASE_PATH)) {
      String appPath = path + "/" + appId;
      assertFalse(new File(appPath).exists());
    }
  }

  @Test
  public void clearTest() throws Exception {
    ShuffleServerConf conf = new ShuffleServerConf();
    String storageBasePath = HDFS_URI + "rss/clearTest";
    final int shuffleId = 1;
    conf.set(ShuffleServerConf.RPC_SERVER_PORT, 1234);
    conf.set(ShuffleServerConf.RSS_COORDINATOR_QUORUM, "localhost:9527");
    conf.set(ShuffleServerConf.JETTY_HTTP_PORT, 12345);
    conf.set(ShuffleServerConf.JETTY_CORE_POOL_SIZE, 64);
    conf.set(ShuffleServerConf.SERVER_BUFFER_CAPACITY, 128L);
    conf.set(ShuffleServerConf.SERVER_MEMORY_SHUFFLE_HIGHWATERMARK_PERCENTAGE, 50.0);
    conf.set(ShuffleServerConf.SERVER_MEMORY_SHUFFLE_LOWWATERMARK_PERCENTAGE, 0.0);
    conf.set(ShuffleServerConf.RSS_STORAGE_BASE_PATH, Arrays.asList(storageBasePath));
    conf.setString(ShuffleServerConf.RSS_STORAGE_TYPE.key(), StorageType.HDFS.name());
    conf.set(ShuffleServerConf.RSS_TEST_MODE_ENABLE, true);
    conf.set(ShuffleServerConf.SERVER_COMMIT_TIMEOUT, 10000L);
    conf.set(ShuffleServerConf.SERVER_APP_EXPIRED_WITHOUT_HEARTBEAT, 2000L);
    conf.set(ShuffleServerConf.HEALTH_CHECK_ENABLE, false);

    shuffleServer = new ShuffleServer(conf);
    ShuffleTaskManager shuffleTaskManager = shuffleServer.getShuffleTaskManager();
    shuffleTaskManager.registerShuffle(
        "clearTest1",
        shuffleId,
        Lists.newArrayList(new PartitionRange(0, 1)),
        RemoteStorageInfo.EMPTY_REMOTE_STORAGE,
        StringUtils.EMPTY);
    shuffleTaskManager.registerShuffle(
        "clearTest2",
        shuffleId,
        Lists.newArrayList(new PartitionRange(0, 1)),
        RemoteStorageInfo.EMPTY_REMOTE_STORAGE,
        StringUtils.EMPTY);
    shuffleTaskManager.refreshAppId("clearTest1");
    shuffleTaskManager.refreshAppId("clearTest2");
    assertEquals(2, shuffleTaskManager.getAppIds().size());

    // keep refresh status of application "clearTest1"
    int retry = 0;
    while (retry < 10) {
      Thread.sleep(1000);
      ShufflePartitionedData partitionedData0 = createPartitionedData(1, 1, 35);
      shuffleTaskManager.cacheShuffleData("clearTest1", shuffleId, false, partitionedData0);
      shuffleTaskManager.updateCachedBlockIds("clearTest1", shuffleId, partitionedData0);
      shuffleTaskManager.refreshAppId("clearTest1");
      shuffleTaskManager.checkResourceStatus();
      retry++;
    }
    // application "clearTest2" was removed according to rss.server.app.expired.withoutHeartbeat
    assertEquals(Sets.newHashSet("clearTest1"), shuffleTaskManager.getAppIds());
    assertEquals(10, shuffleTaskManager.getCachedBlockCount("clearTest1", shuffleId));

    // register again
    shuffleTaskManager.registerShuffle(
        "clearTest2",
        shuffleId,
        Lists.newArrayList(new PartitionRange(0, 1)),
        RemoteStorageInfo.EMPTY_REMOTE_STORAGE,
        StringUtils.EMPTY);
    shuffleTaskManager.refreshAppId("clearTest2");
    shuffleTaskManager.checkResourceStatus();
    assertEquals(Sets.newHashSet("clearTest1", "clearTest2"), shuffleTaskManager.getAppIds());
    Thread.sleep(5000);
    shuffleTaskManager.checkResourceStatus();
    // wait resource delete
    Thread.sleep(3000);
    assertEquals(Collections.EMPTY_SET, shuffleTaskManager.getAppIds());
    assertEquals(0, shuffleTaskManager.getCachedBlockCount("clearTest1", shuffleId));
  }

  @Test
  public void clearMultiTimesTest() throws Exception {
    ShuffleServerConf conf = new ShuffleServerConf();
    String storageBasePath = HDFS_URI + "rss/clearMultiTimesTest";
    final int shuffleId = 1;
    conf.set(ShuffleServerConf.RPC_SERVER_PORT, 1234);
    conf.set(ShuffleServerConf.RSS_COORDINATOR_QUORUM, "localhost:9527");
    conf.set(ShuffleServerConf.JETTY_HTTP_PORT, 12345);
    conf.set(ShuffleServerConf.JETTY_CORE_POOL_SIZE, 64);
    conf.set(ShuffleServerConf.SERVER_BUFFER_CAPACITY, 128L);
    conf.set(ShuffleServerConf.SERVER_MEMORY_SHUFFLE_HIGHWATERMARK_PERCENTAGE, 50.0);
    conf.set(ShuffleServerConf.SERVER_MEMORY_SHUFFLE_LOWWATERMARK_PERCENTAGE, 0.0);
    conf.set(ShuffleServerConf.RSS_STORAGE_BASE_PATH, Arrays.asList(storageBasePath));
    conf.setString(ShuffleServerConf.RSS_STORAGE_TYPE.key(), StorageType.HDFS.name());
    conf.set(ShuffleServerConf.RSS_TEST_MODE_ENABLE, true);
    conf.set(ShuffleServerConf.SERVER_COMMIT_TIMEOUT, 10000L);
    conf.set(ShuffleServerConf.SERVER_APP_EXPIRED_WITHOUT_HEARTBEAT, 2000L);
    conf.set(ShuffleServerConf.HEALTH_CHECK_ENABLE, false);

    shuffleServer = new ShuffleServer(conf);
    ShuffleTaskManager shuffleTaskManager = shuffleServer.getShuffleTaskManager();
    String appId = "clearMultiTimesTest";
    shuffleTaskManager.registerShuffle(
        appId,
        shuffleId,
        Lists.newArrayList(new PartitionRange(0, 1)),
        RemoteStorageInfo.EMPTY_REMOTE_STORAGE,
        StringUtils.EMPTY);
    shuffleTaskManager.refreshAppId(appId);
    assertEquals(1, shuffleTaskManager.getAppIds().size());

    shuffleTaskManager.checkResourceStatus();
    assertEquals(Sets.newHashSet(appId), shuffleTaskManager.getAppIds());

    CountDownLatch countDownLatch = new CountDownLatch(3);
    for (int i = 0; i < 3; i++) {
      new Thread(
              () -> {
                try {
                  shuffleTaskManager.removeResources(appId, false);
                } finally {
                  countDownLatch.countDown();
                }
              })
          .start();
    }
    countDownLatch.await();
    assertEquals(Collections.EMPTY_SET, shuffleTaskManager.getAppIds());
    assertEquals(0, shuffleTaskManager.getCachedBlockCount(appId, shuffleId));
  }

  @Test
  public void removeResourcesByShuffleIdsMultiTimesTest() throws Exception {
    ShuffleServerConf conf = new ShuffleServerConf();
    String storageBasePath = HDFS_URI + "rss/removeResourcesByShuffleIdsMultiTimesTest";
    final int shuffleId = 1;
    conf.set(ShuffleServerConf.RPC_SERVER_PORT, 1234);
    conf.set(ShuffleServerConf.RSS_COORDINATOR_QUORUM, "localhost:9527");
    conf.set(ShuffleServerConf.JETTY_HTTP_PORT, 12345);
    conf.set(ShuffleServerConf.JETTY_CORE_POOL_SIZE, 64);
    conf.set(ShuffleServerConf.SERVER_BUFFER_CAPACITY, 128L);
    conf.set(ShuffleServerConf.SERVER_MEMORY_SHUFFLE_HIGHWATERMARK_PERCENTAGE, 50.0);
    conf.set(ShuffleServerConf.SERVER_MEMORY_SHUFFLE_LOWWATERMARK_PERCENTAGE, 0.0);
    conf.set(ShuffleServerConf.RSS_STORAGE_BASE_PATH, Arrays.asList(storageBasePath));
    conf.setString(ShuffleServerConf.RSS_STORAGE_TYPE.key(), StorageType.HDFS.name());
    conf.set(ShuffleServerConf.RSS_TEST_MODE_ENABLE, true);
    conf.set(ShuffleServerConf.SERVER_COMMIT_TIMEOUT, 10000L);
    conf.set(ShuffleServerConf.SERVER_APP_EXPIRED_WITHOUT_HEARTBEAT, 2000L);
    conf.set(ShuffleServerConf.HEALTH_CHECK_ENABLE, false);

    shuffleServer = new ShuffleServer(conf);
    ShuffleTaskManager shuffleTaskManager = shuffleServer.getShuffleTaskManager();
    String appId = "removeResourcesByShuffleIdsMultiTimesTest";
    shuffleTaskManager.registerShuffle(
        appId,
        shuffleId,
        Lists.newArrayList(new PartitionRange(0, 1)),
        RemoteStorageInfo.EMPTY_REMOTE_STORAGE,
        StringUtils.EMPTY);
    shuffleTaskManager.refreshAppId(appId);
    assertEquals(1, shuffleTaskManager.getAppIds().size());

    shuffleTaskManager.checkResourceStatus();
    assertEquals(Sets.newHashSet(appId), shuffleTaskManager.getAppIds());
    assertEquals(1, (int) ShuffleServerMetrics.gaugeTotalPartitionNum.get());
    CountDownLatch countDownLatch = new CountDownLatch(3);
    List<Integer> shuffleIds = Lists.newArrayList(shuffleId);
    for (int i = 0; i < 3; i++) {
      new Thread(
              () -> {
                try {
                  shuffleTaskManager.removeResourcesByShuffleIds(appId, shuffleIds);
                } finally {
                  countDownLatch.countDown();
                }
              })
          .start();
    }
    countDownLatch.await();
    assertEquals(0, (int) ShuffleServerMetrics.gaugeTotalPartitionNum.get());
  }

  @Test
  public void getBlockIdsByPartitionIdTest() {
    BlockIdLayout layout = BlockIdLayout.DEFAULT;
    ShuffleServerConf conf = new ShuffleServerConf();
    ShuffleTaskManager shuffleTaskManager = new ShuffleTaskManager(conf, null, null, null);

    Roaring64NavigableMap expectedBlockIds = Roaring64NavigableMap.bitmapOf();
    int expectedPartitionId = 5;
    Roaring64NavigableMap bitmapBlockIds = Roaring64NavigableMap.bitmapOf();
    for (int taskId = 1; taskId < 10; taskId++) {
      for (int partitionId = 1; partitionId < 10; partitionId++) {
        for (int i = 0; i < 2; i++) {
          long blockId = layout.getBlockId(i, partitionId, taskId);
          bitmapBlockIds.addLong(blockId);
          if (partitionId == expectedPartitionId) {
            expectedBlockIds.addLong(blockId);
          }
        }
      }
    }
    Roaring64NavigableMap resultBlockIds =
        DefaultShuffleBlockIdManager.getBlockIdsByPartitionId(
            Sets.newHashSet(expectedPartitionId),
            bitmapBlockIds,
            Roaring64NavigableMap.bitmapOf(),
            layout);
    assertEquals(expectedBlockIds, resultBlockIds);

    bitmapBlockIds.addLong(layout.getBlockId(0, 0, 0));
    resultBlockIds =
        DefaultShuffleBlockIdManager.getBlockIdsByPartitionId(
            Sets.newHashSet(0), bitmapBlockIds, Roaring64NavigableMap.bitmapOf(), layout);
    assertEquals(Roaring64NavigableMap.bitmapOf(0L), resultBlockIds);

    long expectedBlockId =
        layout.getBlockId(layout.maxSequenceNo, layout.maxPartitionId, layout.maxTaskAttemptId);
    bitmapBlockIds.addLong(expectedBlockId);
    resultBlockIds =
        DefaultShuffleBlockIdManager.getBlockIdsByPartitionId(
            Sets.newHashSet(layout.maxPartitionId),
            bitmapBlockIds,
            Roaring64NavigableMap.bitmapOf(),
            layout);
    assertEquals(Roaring64NavigableMap.bitmapOf(expectedBlockId), resultBlockIds);
  }

  @Test
  public void getBlockIdsByMultiPartitionTest() {
    BlockIdLayout layout = BlockIdLayout.DEFAULT;
    ShuffleServerConf conf = new ShuffleServerConf();
    ShuffleTaskManager shuffleTaskManager = new ShuffleTaskManager(conf, null, null, null);

    Roaring64NavigableMap expectedBlockIds = Roaring64NavigableMap.bitmapOf();
    int startPartition = 3;
    int endPartition = 5;
    Roaring64NavigableMap bitmapBlockIds = Roaring64NavigableMap.bitmapOf();
    for (int taskId = 1; taskId < 10; taskId++) {
      for (int partitionId = 1; partitionId < 10; partitionId++) {
        for (int i = 0; i < 2; i++) {
          long blockId = layout.getBlockId(i, partitionId, taskId);
          bitmapBlockIds.addLong(blockId);
          if (partitionId >= startPartition && partitionId <= endPartition) {
            expectedBlockIds.addLong(blockId);
          }
        }
      }
    }
    Set<Integer> requestPartitions = Sets.newHashSet();
    Set<Integer> allPartitions = Sets.newHashSet();
    for (int partitionId = 1; partitionId < 10; partitionId++) {
      allPartitions.add(partitionId);
      if (partitionId >= startPartition && partitionId <= endPartition) {
        requestPartitions.add(partitionId);
      }
    }

    Roaring64NavigableMap resultBlockIds =
        DefaultShuffleBlockIdManager.getBlockIdsByPartitionId(
            requestPartitions, bitmapBlockIds, Roaring64NavigableMap.bitmapOf(), layout);
    assertEquals(expectedBlockIds, resultBlockIds);
    assertEquals(
        bitmapBlockIds,
        DefaultShuffleBlockIdManager.getBlockIdsByPartitionId(
            allPartitions, bitmapBlockIds, Roaring64NavigableMap.bitmapOf(), layout));
  }

  @Test
  public void testGetFinishedBlockIds() throws Exception {
    ShuffleServerConf conf = new ShuffleServerConf();
    String storageBasePath = HDFS_URI + "rss/test";
    String appId = "test_app";
    final int shuffleId = 1;
    final int bitNum = 3;
    final int partitionNum = 10;
    final int taskNum = 10;
    final int blocksPerTask = 2;
    conf.set(ShuffleServerConf.RPC_SERVER_PORT, 1234);
    conf.set(ShuffleServerConf.RSS_COORDINATOR_QUORUM, "localhost:9527");
    conf.set(ShuffleServerConf.JETTY_HTTP_PORT, 12345);
    conf.set(ShuffleServerConf.JETTY_CORE_POOL_SIZE, 64);
    conf.set(ShuffleServerConf.SERVER_BUFFER_CAPACITY, 128L);
    conf.set(ShuffleServerConf.SERVER_MEMORY_SHUFFLE_HIGHWATERMARK_PERCENTAGE, 50.0);
    conf.set(ShuffleServerConf.SERVER_MEMORY_SHUFFLE_LOWWATERMARK_PERCENTAGE, 0.0);
    conf.set(ShuffleServerConf.RSS_STORAGE_BASE_PATH, Arrays.asList(storageBasePath));
    conf.setString(ShuffleServerConf.RSS_STORAGE_TYPE.key(), StorageType.HDFS.name());
    conf.set(ShuffleServerConf.RSS_TEST_MODE_ENABLE, true);
    conf.set(ShuffleServerConf.SERVER_COMMIT_TIMEOUT, 10000L);
    conf.set(ShuffleServerConf.SERVER_APP_EXPIRED_WITHOUT_HEARTBEAT, 2000L);
    conf.set(ShuffleServerConf.HEALTH_CHECK_ENABLE, false);

    shuffleServer = new ShuffleServer(conf);
    ShuffleBufferManager shuffleBufferManager = shuffleServer.getShuffleBufferManager();
    ShuffleFlushManager shuffleFlushManager = shuffleServer.getShuffleFlushManager();
    StorageManager storageManager = shuffleServer.getStorageManager();
    ShuffleTaskManager shuffleTaskManager =
        new ShuffleTaskManager(conf, shuffleFlushManager, shuffleBufferManager, storageManager);

    int startPartition = 6;
    int endPartition = 9;
    BlockIdLayout layout = BlockIdLayout.DEFAULT;
    Roaring64NavigableMap expectedBlockIds = Roaring64NavigableMap.bitmapOf();
    Map<Integer, long[]> blockIdsToReport = Maps.newHashMap();

    for (int partitionId = 0; partitionId < partitionNum; partitionId++) {
      shuffleTaskManager.registerShuffle(
          appId,
          shuffleId,
          Lists.newArrayList(new PartitionRange(partitionId, partitionId)),
          new RemoteStorageInfo(storageBasePath),
          StringUtils.EMPTY);
      long[] blockIds = new long[taskNum * blocksPerTask];
      for (int taskId = 0; taskId < taskNum; taskId++) {
        for (int i = 0; i < blocksPerTask; i++) {
          long blockId = layout.getBlockId(i, partitionId, taskId);
          blockIds[taskId * blocksPerTask + i] = blockId;
        }
      }
      blockIdsToReport.putIfAbsent(partitionId, blockIds);
      if (partitionId >= startPartition) {
        expectedBlockIds.add(blockIds);
      }
    }
    assertEquals(
        (endPartition - startPartition + 1) * taskNum * blocksPerTask,
        expectedBlockIds.getLongCardinality());

    shuffleTaskManager.addFinishedBlockIds(appId, shuffleId, blockIdsToReport, bitNum);
    Set<Integer> requestPartitions = Sets.newHashSet();
    for (int partitionId = startPartition; partitionId <= endPartition; partitionId++) {
      requestPartitions.add(partitionId);
    }
    byte[] serializeBitMap =
        shuffleTaskManager.getFinishedBlockIds(appId, shuffleId, requestPartitions, layout);
    Roaring64NavigableMap resBlockIds = RssUtils.deserializeBitMap(serializeBitMap);
    assertEquals(expectedBlockIds, resBlockIds);

    try {
      // calling with same appId and shuffleId but different bitmapNum should fail
      shuffleTaskManager.addFinishedBlockIds(appId, shuffleId, blockIdsToReport, bitNum - 1);
      fail("Exception should be thrown");
    } catch (InvalidRequestException e) {
      assertEquals(e.getMessage(), "Request expects 2 bitmaps, but there are 3 bitmaps!");
    }
  }

  @Test
  public void testAddFinishedBlockIdsWithoutRegister() throws Exception {
    ShuffleServerConf conf = new ShuffleServerConf();
    String storageBasePath = HDFS_URI + "rss/test";
    String appId = "testAddFinishedBlockIdsToExpiredApp";
    final int shuffleId = 1;
    final int bitNum = 3;
    conf.set(ShuffleServerConf.RPC_SERVER_PORT, 1234);
    conf.set(ShuffleServerConf.RSS_COORDINATOR_QUORUM, "localhost:9527");
    conf.set(ShuffleServerConf.JETTY_HTTP_PORT, 12345);
    conf.set(ShuffleServerConf.JETTY_CORE_POOL_SIZE, 64);
    conf.set(ShuffleServerConf.SERVER_BUFFER_CAPACITY, 128L);
    conf.set(ShuffleServerConf.SERVER_MEMORY_SHUFFLE_HIGHWATERMARK_PERCENTAGE, 50.0);
    conf.set(ShuffleServerConf.SERVER_MEMORY_SHUFFLE_LOWWATERMARK_PERCENTAGE, 0.0);
    conf.set(ShuffleServerConf.RSS_STORAGE_BASE_PATH, Arrays.asList(storageBasePath));
    conf.setString(ShuffleServerConf.RSS_STORAGE_TYPE.key(), StorageType.HDFS.name());
    conf.set(ShuffleServerConf.RSS_TEST_MODE_ENABLE, true);
    conf.set(ShuffleServerConf.SERVER_COMMIT_TIMEOUT, 10000L);
    conf.set(ShuffleServerConf.SERVER_APP_EXPIRED_WITHOUT_HEARTBEAT, 2000L);
    conf.set(ShuffleServerConf.HEALTH_CHECK_ENABLE, false);

    shuffleServer = new ShuffleServer(conf);
    ShuffleBufferManager shuffleBufferManager = shuffleServer.getShuffleBufferManager();
    ShuffleFlushManager shuffleFlushManager = shuffleServer.getShuffleFlushManager();
    StorageManager storageManager = shuffleServer.getStorageManager();
    ShuffleTaskManager shuffleTaskManager =
        new ShuffleTaskManager(conf, shuffleFlushManager, shuffleBufferManager, storageManager);
    Map<Integer, long[]> blockIdsToReport = Maps.newHashMap();
    try {
      shuffleTaskManager.addFinishedBlockIds(appId, shuffleId, blockIdsToReport, bitNum);
      fail("Exception should be thrown");
    } catch (RuntimeException e) {
      assertTrue(e.getMessage().equals("appId[" + appId + "] is expired!"));
    }
  }

  @Test
  public void reportShuffleResultShouldIgnoreStaleStageAfterShuffleCleanup() throws Exception {
    ShuffleServerConf conf = constructServerConfWithLocalfile();
    shuffleServer = new ShuffleServer(conf);
    ShuffleTaskManager shuffleTaskManager = shuffleServer.getShuffleTaskManager();
    String appId = "testIgnoreStaleStageAfterCleanup";
    int shuffleId = 1;
    int bitmapNum = 3;
    BlockIdLayout layout = BlockIdLayout.DEFAULT;
    Set<Integer> partitions = Sets.newHashSet(0);

    shuffleTaskManager.registerShuffle(
        appId,
        shuffleId,
        Lists.newArrayList(new PartitionRange(0, 0)),
        new RemoteStorageInfo(tempDir1.getAbsolutePath()),
        StringUtils.EMPTY);

    Map<Integer, long[]> stage0Blocks = Maps.newHashMap();
    stage0Blocks.put(0, new long[] {layout.getBlockId(0, 0, 0), layout.getBlockId(1, 0, 0)});
    assertEquals(
        2,
        shuffleTaskManager.addFinishedBlockIds(
            appId, shuffleId, stage0Blocks, bitmapNum, true, 0));

    Roaring64NavigableMap stage0Result =
        RssUtils.deserializeBitMap(
            shuffleTaskManager.getFinishedBlockIds(appId, shuffleId, partitions, layout));
    assertEquals(2, stage0Result.getLongCardinality());

    shuffleTaskManager.removeShuffleDataSync(appId, shuffleId);

    assertEquals(
        0,
        shuffleTaskManager.addFinishedBlockIds(
            appId, shuffleId, stage0Blocks, bitmapNum, true, 0));
    assertEquals(0, shuffleTaskManager.addFinishedBlockIds(appId, shuffleId, stage0Blocks, bitmapNum));

    Map<Integer, long[]> stage1Blocks = Maps.newHashMap();
    long stage1BlockId = layout.getBlockId(0, 0, 1);
    stage1Blocks.put(0, new long[] {stage1BlockId});
    assertEquals(
        1,
        shuffleTaskManager.addFinishedBlockIds(
            appId, shuffleId, stage1Blocks, bitmapNum, true, 1));

    Roaring64NavigableMap stage1Result =
        RssUtils.deserializeBitMap(
            shuffleTaskManager.getFinishedBlockIds(appId, shuffleId, partitions, layout));
    assertEquals(Roaring64NavigableMap.bitmapOf(stage1BlockId), stage1Result);
  }

  @Test
  public void newerStageReportShouldReplaceOlderStageResult() throws Exception {
    ShuffleServerConf conf = constructServerConfWithLocalfile();
    shuffleServer = new ShuffleServer(conf);
    ShuffleTaskManager shuffleTaskManager = shuffleServer.getShuffleTaskManager();
    String appId = "testNewerStageReportShouldReplaceOlderStageResult";
    int shuffleId = 1;
    int bitmapNum = 3;
    BlockIdLayout layout = BlockIdLayout.DEFAULT;
    Set<Integer> partitions = Sets.newHashSet(0);

    shuffleTaskManager.registerShuffle(
        appId,
        shuffleId,
        Lists.newArrayList(new PartitionRange(0, 0)),
        new RemoteStorageInfo(tempDir1.getAbsolutePath()),
        StringUtils.EMPTY);

    Map<Integer, long[]> stage0Blocks = Maps.newHashMap();
    stage0Blocks.put(0, new long[] {layout.getBlockId(0, 0, 0)});
    assertEquals(
        1,
        shuffleTaskManager.addFinishedBlockIds(
            appId, shuffleId, stage0Blocks, bitmapNum, true, 0));

    Map<Integer, long[]> stage1Blocks = Maps.newHashMap();
    long stage1BlockId = layout.getBlockId(0, 0, 1);
    stage1Blocks.put(0, new long[] {stage1BlockId});
    assertEquals(
        1,
        shuffleTaskManager.addFinishedBlockIds(
            appId, shuffleId, stage1Blocks, bitmapNum, true, 1));

    Roaring64NavigableMap stage1Result =
        RssUtils.deserializeBitMap(
            shuffleTaskManager.getFinishedBlockIds(appId, shuffleId, partitions, layout));
    assertEquals(Roaring64NavigableMap.bitmapOf(stage1BlockId), stage1Result);
    assertEquals(1, shuffleTaskManager.getShuffleTaskInfo(appId).getBlockNumber(shuffleId, 0));
  }

  @Test
  public void staleShuffleCleanupShouldNotRemoveNewerStageAttemptResult() throws Exception {
    ShuffleServerConf conf = constructServerConfWithLocalfile();
    shuffleServer = new ShuffleServer(conf);
    ShuffleTaskManager shuffleTaskManager = shuffleServer.getShuffleTaskManager();
    ShuffleServerGrpcService grpcService = new ShuffleServerGrpcService(shuffleServer);
    String appId = "testStaleCleanupShouldNotRemoveNewAttempt";
    int shuffleId = 1;
    int bitmapNum = 3;
    BlockIdLayout layout = BlockIdLayout.DEFAULT;
    Set<Integer> partitions = Sets.newHashSet(0);

    shuffleTaskManager.registerShuffle(
        appId,
        shuffleId,
        Lists.newArrayList(new PartitionRange(0, 0)),
        new RemoteStorageInfo(tempDir1.getAbsolutePath()),
        StringUtils.EMPTY);

    Map<Integer, long[]> stage1Blocks = Maps.newHashMap();
    long stage1BlockId = layout.getBlockId(0, 0, 1);
    stage1Blocks.put(0, new long[] {stage1BlockId});
    assertEquals(
        1,
        shuffleTaskManager.addFinishedBlockIds(
            appId, shuffleId, stage1Blocks, bitmapNum, true, 1));

    CapturingStreamObserver<RssProtos.ShuffleUnregisterResponse> observer =
        new CapturingStreamObserver<>();
    grpcService.unregisterShuffle(
        RssProtos.ShuffleUnregisterRequest.newBuilder()
            .setAppId(appId)
            .setShuffleId(shuffleId)
            .setStageAttemptNumber(0)
            .build(),
        observer);

    assertNull(observer.error);
    assertTrue(observer.completed);
    assertEquals(RssProtos.StatusCode.SUCCESS, observer.value.getStatus());
    assertFalse(observer.value.getStageAttemptCleanupCompleted());

    Roaring64NavigableMap stage1Result =
        RssUtils.deserializeBitMap(
            shuffleTaskManager.getFinishedBlockIds(appId, shuffleId, partitions, layout));
    assertEquals(Roaring64NavigableMap.bitmapOf(stage1BlockId), stage1Result);

    Map<Integer, long[]> stage0Blocks = Maps.newHashMap();
    stage0Blocks.put(0, new long[] {layout.getBlockId(0, 0, 0)});
    assertEquals(0, shuffleTaskManager.addFinishedBlockIds(appId, shuffleId, stage0Blocks, bitmapNum));
    assertEquals(
        0,
        shuffleTaskManager.addFinishedBlockIds(
            appId, shuffleId, stage0Blocks, bitmapNum, true, 0));
  }

  @Test
  public void stageAwareUnregisterShouldCleanRemoteMergeByMergeAppFence() throws Exception {
    ShuffleServerConf conf = constructServerConfWithLocalfile();
    conf.set(ShuffleServerConf.SERVER_MERGE_ENABLE, true);
    conf.set(ShuffleServerConf.SERVER_SHUFFLE_BUFFER_TYPE, ShuffleBufferType.SKIP_LIST);
    shuffleServer = new ShuffleServer(conf);
    ShuffleTaskManager shuffleTaskManager = shuffleServer.getShuffleTaskManager();
    ShuffleServerGrpcService grpcService = new ShuffleServerGrpcService(shuffleServer);
    String appId = "testStageAwareUnregisterShouldCleanRemoteMergeByMergeFence";
    String mergeAppId = appId + MERGE_APP_SUFFIX;
    int shuffleId = 1;
    int bitmapNum = 3;
    BlockIdLayout layout = BlockIdLayout.DEFAULT;
    Set<Integer> partitions = Sets.newHashSet(0);
    List<PartitionRange> partitionRanges = Lists.newArrayList(new PartitionRange(0, 0));
    RemoteStorageInfo remoteStorageInfo = new RemoteStorageInfo(tempDir1.getAbsolutePath());

    shuffleTaskManager.registerShuffle(
        appId, shuffleId, partitionRanges, remoteStorageInfo, StringUtils.EMPTY);
    shuffleTaskManager.registerShuffle(
        mergeAppId, shuffleId, partitionRanges, remoteStorageInfo, StringUtils.EMPTY);

    Map<Integer, long[]> mainStage1Blocks = Maps.newHashMap();
    long mainStage1BlockId = layout.getBlockId(0, 0, 1);
    mainStage1Blocks.put(0, new long[] {mainStage1BlockId});
    assertEquals(
        1,
        shuffleTaskManager.addFinishedBlockIds(
            appId, shuffleId, mainStage1Blocks, bitmapNum, true, 1));

    Map<Integer, long[]> mergeBlocks = Maps.newHashMap();
    long mergeBlockId = layout.getBlockId(1, 0, 0);
    mergeBlocks.put(0, new long[] {mergeBlockId});
    assertEquals(
        1,
        shuffleTaskManager.addFinishedBlockIds(
            mergeAppId, shuffleId, mergeBlocks, bitmapNum, true, 0));

    CapturingStreamObserver<RssProtos.ShuffleUnregisterResponse> observer =
        new CapturingStreamObserver<>();
    grpcService.unregisterShuffle(
        RssProtos.ShuffleUnregisterRequest.newBuilder()
            .setAppId(appId)
            .setShuffleId(shuffleId)
            .setStageAttemptNumber(0)
            .build(),
        observer);

    assertNull(observer.error);
    assertTrue(observer.completed);
    assertEquals(RssProtos.StatusCode.SUCCESS, observer.value.getStatus());
    assertFalse(observer.value.getStageAttemptCleanupCompleted());
    Roaring64NavigableMap mergeResult =
        RssUtils.deserializeBitMap(
            shuffleTaskManager.getFinishedBlockIds(mergeAppId, shuffleId, partitions, layout));
    assertEquals(0, mergeResult.getLongCardinality());
    Roaring64NavigableMap mainResult =
        RssUtils.deserializeBitMap(
            shuffleTaskManager.getFinishedBlockIds(appId, shuffleId, partitions, layout));
    assertEquals(Roaring64NavigableMap.bitmapOf(mainStage1BlockId), mainResult);

    Map<Integer, long[]> stage0Blocks = Maps.newHashMap();
    stage0Blocks.put(0, new long[] {layout.getBlockId(0, 0, 0)});
    assertEquals(
        0,
        shuffleTaskManager.addFinishedBlockIds(
            mergeAppId, shuffleId, stage0Blocks, bitmapNum, true, 0));
  }

  @Test
  public void stageAwareUnregisterShouldCleanRemoteMergeWhenNoNewerStageExists() throws Exception {
    ShuffleServerConf conf = constructServerConfWithLocalfile();
    conf.set(ShuffleServerConf.SERVER_MERGE_ENABLE, true);
    conf.set(ShuffleServerConf.SERVER_SHUFFLE_BUFFER_TYPE, ShuffleBufferType.SKIP_LIST);
    shuffleServer = new ShuffleServer(conf);
    ShuffleTaskManager shuffleTaskManager = shuffleServer.getShuffleTaskManager();
    ShuffleServerGrpcService grpcService = new ShuffleServerGrpcService(shuffleServer);
    String appId = "testStageAwareUnregisterShouldCleanRemoteMerge";
    String mergeAppId = appId + MERGE_APP_SUFFIX;
    int shuffleId = 1;
    int bitmapNum = 3;
    BlockIdLayout layout = BlockIdLayout.DEFAULT;
    Set<Integer> partitions = Sets.newHashSet(0);
    List<PartitionRange> partitionRanges = Lists.newArrayList(new PartitionRange(0, 0));
    RemoteStorageInfo remoteStorageInfo = new RemoteStorageInfo(tempDir1.getAbsolutePath());

    shuffleTaskManager.registerShuffle(
        appId, shuffleId, partitionRanges, remoteStorageInfo, StringUtils.EMPTY);
    shuffleTaskManager.registerShuffle(
        mergeAppId, shuffleId, partitionRanges, remoteStorageInfo, StringUtils.EMPTY);

    Map<Integer, long[]> mainStage0Blocks = Maps.newHashMap();
    mainStage0Blocks.put(0, new long[] {layout.getBlockId(0, 0, 0)});
    assertEquals(
        1,
        shuffleTaskManager.addFinishedBlockIds(
            appId, shuffleId, mainStage0Blocks, bitmapNum, true, 0));

    Map<Integer, long[]> mergeStage0Blocks = Maps.newHashMap();
    mergeStage0Blocks.put(0, new long[] {layout.getBlockId(1, 0, 0)});
    assertEquals(
        1,
        shuffleTaskManager.addFinishedBlockIds(
            mergeAppId, shuffleId, mergeStage0Blocks, bitmapNum));

    CapturingStreamObserver<RssProtos.ShuffleUnregisterResponse> observer =
        new CapturingStreamObserver<>();
    grpcService.unregisterShuffle(
        RssProtos.ShuffleUnregisterRequest.newBuilder()
            .setAppId(appId)
            .setShuffleId(shuffleId)
            .setStageAttemptNumber(0)
            .build(),
        observer);

    assertNull(observer.error);
    assertTrue(observer.completed);
    assertEquals(RssProtos.StatusCode.SUCCESS, observer.value.getStatus());
    assertTrue(observer.value.getStageAttemptCleanupCompleted());
    Roaring64NavigableMap mergeResult =
        RssUtils.deserializeBitMap(
            shuffleTaskManager.getFinishedBlockIds(mergeAppId, shuffleId, partitions, layout));
    assertEquals(0, mergeResult.getLongCardinality());
    assertEquals(
        0,
        shuffleTaskManager.addFinishedBlockIds(
            mergeAppId, shuffleId, mergeStage0Blocks, bitmapNum, true, 0));
  }

  @Test
  public void stageAwareUnregisterShouldNotAckWhenStoragePurgeFails() throws Exception {
    ShuffleServerConf conf = constructServerConfWithLocalfile();
    shuffleServer = new ShuffleServer(conf);
    ShuffleTaskManager shuffleTaskManager = shuffleServer.getShuffleTaskManager();
    LocalStorageManager storageManager = (LocalStorageManager) shuffleServer.getStorageManager();
    storageManager = spy(storageManager);
    doAnswer(
            invocation -> {
              throw new RssException("purge failed");
            })
        .when(storageManager)
        .removeResources(any(PurgeEvent.class));
    shuffleTaskManager.setStorageManager(storageManager);
    ShuffleServerGrpcService grpcService = new ShuffleServerGrpcService(shuffleServer);
    String appId = "testStageAwareUnregisterShouldNotAckWhenStoragePurgeFails";
    int shuffleId = 1;
    int bitmapNum = 3;
    BlockIdLayout layout = BlockIdLayout.DEFAULT;

    shuffleTaskManager.registerShuffle(
        appId,
        shuffleId,
        Lists.newArrayList(new PartitionRange(0, 0)),
        new RemoteStorageInfo(tempDir1.getAbsolutePath()),
        StringUtils.EMPTY);

    CapturingStreamObserver<RssProtos.ShuffleUnregisterResponse> observer =
        new CapturingStreamObserver<>();
    grpcService.unregisterShuffle(
        RssProtos.ShuffleUnregisterRequest.newBuilder()
            .setAppId(appId)
            .setShuffleId(shuffleId)
            .setStageAttemptNumber(0)
            .build(),
        observer);

    assertNull(observer.error);
    assertTrue(observer.completed);
    assertEquals(RssProtos.StatusCode.INTERNAL_ERROR, observer.value.getStatus());
    assertFalse(observer.value.getStageAttemptCleanupCompleted());

    Map<Integer, long[]> stage0Blocks = Maps.newHashMap();
    stage0Blocks.put(0, new long[] {layout.getBlockId(0, 0, 0)});
    ShuffleTaskManager.AddFinishedBlockIdsResult addResult =
        shuffleTaskManager.addFinishedBlockIdsWithStatus(
            appId, shuffleId, stage0Blocks, bitmapNum, true, 0);
    assertEquals(StatusCode.STAGE_RETRY_IGNORE, addResult.getStatusCode());
    assertEquals(0, addResult.getUpdatedBlockCount());
    assertEquals(
        StatusCode.STAGE_RETRY_IGNORE,
        shuffleTaskManager.cacheShuffleData(
            appId, shuffleId, 0, false, 0, createPartitionedData(0, 1, 17)));
    assertEquals(0, shuffleTaskManager.getLatestStageAttemptNumber(appId, shuffleId));

    doAnswer(invocation -> null).when(storageManager).removeResources(any(PurgeEvent.class));
    CapturingStreamObserver<RssProtos.ShuffleUnregisterResponse> retryObserver =
        new CapturingStreamObserver<>();
    grpcService.unregisterShuffle(
        RssProtos.ShuffleUnregisterRequest.newBuilder()
            .setAppId(appId)
            .setShuffleId(shuffleId)
            .setStageAttemptNumber(0)
            .build(),
        retryObserver);

    assertNull(retryObserver.error);
    assertTrue(retryObserver.completed);
    assertEquals(RssProtos.StatusCode.SUCCESS, retryObserver.value.getStatus());
    assertTrue(retryObserver.value.getStageAttemptCleanupCompleted());
    verify(storageManager, times(2)).removeResources(any(PurgeEvent.class));
  }

  @Test
  public void failedStageAwareUnregisterShouldNotBlockNextStageAttemptCleanup() throws Exception {
    ShuffleServerConf conf = constructServerConfWithLocalfile();
    shuffleServer = new ShuffleServer(conf);
    ShuffleTaskManager shuffleTaskManager = shuffleServer.getShuffleTaskManager();
    LocalStorageManager storageManager = (LocalStorageManager) shuffleServer.getStorageManager();
    storageManager = spy(storageManager);
    doThrow(new RssException("purge failed")).when(storageManager).removeResources(any(PurgeEvent.class));
    shuffleTaskManager.setStorageManager(storageManager);
    ShuffleServerGrpcService grpcService = new ShuffleServerGrpcService(shuffleServer);
    String appId = "testFailedStageAwareUnregisterShouldNotBlockNextStageAttemptCleanup";
    int shuffleId = 1;

    shuffleTaskManager.registerShuffle(
        appId,
        shuffleId,
        Lists.newArrayList(new PartitionRange(0, 0)),
        new RemoteStorageInfo(tempDir1.getAbsolutePath()),
        StringUtils.EMPTY);
    ShufflePartitionedData stage0Data = createPartitionedData(0, 1, 35);
    assertEquals(
        StatusCode.SUCCESS,
        shuffleTaskManager.cacheShuffleData(appId, shuffleId, 0, false, 0, stage0Data));
    assertEquals(
        stage0Data.getTotalBlockEncodedLength(),
        shuffleTaskManager.getPartitionDataSize(appId, shuffleId, 0));

    CapturingStreamObserver<RssProtos.ShuffleUnregisterResponse> observer =
        new CapturingStreamObserver<>();
    grpcService.unregisterShuffle(
        RssProtos.ShuffleUnregisterRequest.newBuilder()
            .setAppId(appId)
            .setShuffleId(shuffleId)
            .setStageAttemptNumber(0)
            .build(),
        observer);

    assertNull(observer.error);
    assertTrue(observer.completed);
    assertEquals(RssProtos.StatusCode.INTERNAL_ERROR, observer.value.getStatus());
    assertFalse(observer.value.getStageAttemptCleanupCompleted());
    assertEquals(0, shuffleTaskManager.getLatestStageAttemptNumber(appId, shuffleId));

    ShufflePartitionedData stage1Data = createPartitionedData(0, 1, 17);
    assertEquals(
        StatusCode.SUCCESS,
        shuffleTaskManager.cacheShuffleData(appId, shuffleId, 1, false, 0, stage1Data));
    assertEquals(1, shuffleTaskManager.getLatestStageAttemptNumber(appId, shuffleId));
    assertEquals(
        stage1Data.getTotalBlockEncodedLength(),
        shuffleTaskManager.getPartitionDataSize(appId, shuffleId, 0));
  }

  @Test
  public void appRemoveResourcesShouldKeepMetadataWhenStoragePurgeFails() throws Exception {
    ShuffleServerConf conf = constructServerConfWithLocalfile();
    shuffleServer = new ShuffleServer(conf);
    ShuffleTaskManager shuffleTaskManager = shuffleServer.getShuffleTaskManager();
    LocalStorageManager storageManager = (LocalStorageManager) shuffleServer.getStorageManager();
    storageManager = spy(storageManager);
    doAnswer(
            invocation -> {
              throw new RssException("app purge failed");
            })
        .when(storageManager)
        .removeResources(any(PurgeEvent.class));
    shuffleTaskManager.setStorageManager(storageManager);
    String appId = "testAppRemoveResourcesShouldKeepMetadataWhenStoragePurgeFails";
    int shuffleId = 1;

    shuffleTaskManager.registerShuffle(
        appId,
        shuffleId,
        Lists.newArrayList(new PartitionRange(0, 0)),
        new RemoteStorageInfo(tempDir1.getAbsolutePath()),
        StringUtils.EMPTY);
    ShufflePartitionedData partitionedData = createPartitionedData(0, 1, 35);
    shuffleTaskManager.requireBuffer(35);
    shuffleTaskManager.cacheShuffleData(appId, shuffleId, false, partitionedData);
    shuffleTaskManager.updateCachedBlockIds(appId, shuffleId, partitionedData);
    long partitionDataSize = shuffleTaskManager.getPartitionDataSize(appId, shuffleId, 0);

    assertThrows(RssException.class, () -> shuffleTaskManager.removeResources(appId, false));

    assertEquals(Sets.newHashSet(appId), shuffleTaskManager.getAppIds());
    assertNotNull(shuffleTaskManager.getShuffleTaskInfo(appId));
    assertEquals(1, shuffleTaskManager.getCachedBlockCount(appId, shuffleId));
    assertEquals(partitionDataSize, shuffleTaskManager.getPartitionDataSize(appId, shuffleId, 0));
  }

  @Test
  public void commitShuffleTaskShouldIgnoreStaleStageAttempt() throws Exception {
    ShuffleServerConf conf = constructServerConfWithLocalfile();
    shuffleServer = new ShuffleServer(conf);
    ShuffleTaskManager shuffleTaskManager = shuffleServer.getShuffleTaskManager();
    ShuffleServerGrpcService grpcService = new ShuffleServerGrpcService(shuffleServer);
    String appId = "testCommitShuffleTaskShouldIgnoreStaleStageAttempt";
    int shuffleId = 1;
    int bitmapNum = 3;
    BlockIdLayout layout = BlockIdLayout.DEFAULT;

    shuffleTaskManager.registerShuffle(
        appId,
        shuffleId,
        Lists.newArrayList(new PartitionRange(0, 0)),
        new RemoteStorageInfo(tempDir1.getAbsolutePath()),
        StringUtils.EMPTY);

    Map<Integer, long[]> stage1Blocks = Maps.newHashMap();
    stage1Blocks.put(0, new long[] {layout.getBlockId(0, 0, 1)});
    assertEquals(
        1,
        shuffleTaskManager.addFinishedBlockIds(
            appId, shuffleId, stage1Blocks, bitmapNum, true, 1));

    CapturingStreamObserver<RssProtos.ShuffleCommitResponse> staleObserver =
        new CapturingStreamObserver<>();
    grpcService.commitShuffleTask(
        RssProtos.ShuffleCommitRequest.newBuilder()
            .setAppId(appId)
            .setShuffleId(shuffleId)
            .setStageAttemptNumber(0)
            .build(),
        staleObserver);

    assertNull(staleObserver.error);
    assertTrue(staleObserver.completed);
    assertEquals(RssProtos.StatusCode.STAGE_RETRY_IGNORE, staleObserver.value.getStatus());
    assertTrue(staleObserver.value.hasStageAttemptAccepted());
    assertFalse(staleObserver.value.getStageAttemptAccepted());
    assertEquals(0, staleObserver.value.getCommitCount());

    CapturingStreamObserver<RssProtos.ShuffleCommitResponse> currentObserver =
        new CapturingStreamObserver<>();
    grpcService.commitShuffleTask(
        RssProtos.ShuffleCommitRequest.newBuilder()
            .setAppId(appId)
            .setShuffleId(shuffleId)
            .setStageAttemptNumber(1)
            .build(),
        currentObserver);

    assertNull(currentObserver.error);
    assertTrue(currentObserver.completed);
    assertEquals(RssProtos.StatusCode.SUCCESS, currentObserver.value.getStatus());
    assertTrue(currentObserver.value.hasStageAttemptAccepted());
    assertTrue(currentObserver.value.getStageAttemptAccepted());
    assertEquals(1, currentObserver.value.getCommitCount());
  }

  @Test
  @Timeout(10)
  public void stageAwareCleanupShouldInterruptWaitingCommit() throws Exception {
    ShuffleServerConf conf = constructServerConfWithLocalfile();
    conf.set(ShuffleServerConf.SERVER_COMMIT_TIMEOUT, 30000L);
    shuffleServer = new ShuffleServer(conf);
    ShuffleTaskManager shuffleTaskManager = shuffleServer.getShuffleTaskManager();
    String appId = "testStageAwareCleanupShouldInterruptWaitingCommit";
    int shuffleId = 1;

    shuffleTaskManager.registerShuffle(
        appId,
        shuffleId,
        Lists.newArrayList(new PartitionRange(0, 0)),
        new RemoteStorageInfo(tempDir1.getAbsolutePath()),
        StringUtils.EMPTY);
    ShufflePartitionedData partitionedData = createPartitionedData(0, 1, 35);
    shuffleTaskManager.requireBuffer(35);
    assertEquals(
        StatusCode.SUCCESS,
        shuffleTaskManager.cacheShuffleData(appId, shuffleId, false, partitionedData));
    shuffleTaskManager.updateCachedBlockIds(appId, shuffleId, 0, partitionedData);

    CountDownLatch commitCheckStarted = new CountDownLatch(1);
    ShuffleFlushManager spyFlushManager = spy(shuffleServer.getShuffleFlushManager());
    doAnswer(
            invocation -> {
              commitCheckStarted.countDown();
              return 0L;
            })
        .when(spyFlushManager)
        .getCommittedBlockCount(any(String.class), any(Integer.class));
    shuffleTaskManager.setShuffleFlushManager(spyFlushManager);

    StatusCode[] commitStatus = new StatusCode[1];
    Exception[] commitError = new Exception[1];
    Thread commitThread =
        new Thread(
            () -> {
              try {
                commitStatus[0] = shuffleTaskManager.commitShuffle(appId, shuffleId, true, 0);
              } catch (Exception e) {
                commitError[0] = e;
              }
            });
    commitThread.start();

    assertTrue(commitCheckStarted.await(3, TimeUnit.SECONDS));
    assertTrue(shuffleTaskManager.removeShuffleDataSync(appId, shuffleId, 0));
    commitThread.join(TimeUnit.SECONDS.toMillis(3));

    assertFalse(commitThread.isAlive());
    assertNull(commitError[0]);
    assertEquals(StatusCode.STAGE_RETRY_IGNORE, commitStatus[0]);
  }

  @Test
  public void reportShuffleResultShouldRejectStaleStageAttemptAtRpcLayer() throws Exception {
    ShuffleServerConf conf = constructServerConfWithLocalfile();
    shuffleServer = new ShuffleServer(conf);
    ShuffleTaskManager shuffleTaskManager = shuffleServer.getShuffleTaskManager();
    ShuffleServerGrpcService grpcService = new ShuffleServerGrpcService(shuffleServer);
    String appId = "testReportShuffleResultShouldRejectStaleStageAttemptAtRpcLayer";
    int shuffleId = 1;
    BlockIdLayout layout = BlockIdLayout.DEFAULT;

    shuffleTaskManager.registerShuffle(
        appId,
        shuffleId,
        Lists.newArrayList(new PartitionRange(0, 0)),
        new RemoteStorageInfo(tempDir1.getAbsolutePath()),
        StringUtils.EMPTY);

    Map<Integer, long[]> stage1Blocks = Maps.newHashMap();
    stage1Blocks.put(0, new long[] {layout.getBlockId(0, 0, 1)});
    assertEquals(
        1,
        shuffleTaskManager.addFinishedBlockIds(
            appId, shuffleId, stage1Blocks, 3, true, 1));

    CapturingStreamObserver<RssProtos.ReportShuffleResultResponse> staleObserver =
        new CapturingStreamObserver<>();
    grpcService.reportShuffleResult(
        RssProtos.ReportShuffleResultRequest.newBuilder()
            .setAppId(appId)
            .setShuffleId(shuffleId)
            .setBitmapNum(3)
            .setTaskAttemptId(1)
            .setStageAttemptNumber(0)
            .addPartitionToBlockIds(
                RssProtos.PartitionToBlockIds.newBuilder()
                    .setPartitionId(0)
                    .addBlockIds(layout.getBlockId(0, 1, 0)))
            .build(),
        staleObserver);

    assertNull(staleObserver.error);
    assertTrue(staleObserver.completed);
    assertEquals(RssProtos.StatusCode.STAGE_RETRY_IGNORE, staleObserver.value.getStatus());
    assertTrue(staleObserver.value.hasStageAttemptAccepted());
    assertFalse(staleObserver.value.getStageAttemptAccepted());

    CapturingStreamObserver<RssProtos.ReportShuffleResultResponse> duplicateCurrentObserver =
        new CapturingStreamObserver<>();
    grpcService.reportShuffleResult(
        RssProtos.ReportShuffleResultRequest.newBuilder()
            .setAppId(appId)
            .setShuffleId(shuffleId)
            .setBitmapNum(3)
            .setTaskAttemptId(2)
            .setStageAttemptNumber(1)
            .addPartitionToBlockIds(
                RssProtos.PartitionToBlockIds.newBuilder()
                    .setPartitionId(0)
                    .addBlockIds(layout.getBlockId(0, 0, 1)))
            .build(),
        duplicateCurrentObserver);

    assertNull(duplicateCurrentObserver.error);
    assertTrue(duplicateCurrentObserver.completed);
    assertEquals(RssProtos.StatusCode.SUCCESS, duplicateCurrentObserver.value.getStatus());
    assertTrue(duplicateCurrentObserver.value.hasStageAttemptAccepted());
    assertTrue(duplicateCurrentObserver.value.getStageAttemptAccepted());
  }

  @Test
  public void newerStageAttemptShouldResetOldCommitCount() throws Exception {
    ShuffleServerConf conf = constructServerConfWithLocalfile();
    shuffleServer = new ShuffleServer(conf);
    ShuffleTaskManager shuffleTaskManager = shuffleServer.getShuffleTaskManager();
    ShuffleServerGrpcService grpcService = new ShuffleServerGrpcService(shuffleServer);
    String appId = "testNewerStageAttemptShouldResetOldCommitCount";
    int shuffleId = 1;
    BlockIdLayout layout = BlockIdLayout.DEFAULT;

    shuffleTaskManager.registerShuffle(
        appId,
        shuffleId,
        Lists.newArrayList(new PartitionRange(0, 0)),
        new RemoteStorageInfo(tempDir1.getAbsolutePath()),
        StringUtils.EMPTY);

    CapturingStreamObserver<RssProtos.ShuffleCommitResponse> oldCommitObserver =
        new CapturingStreamObserver<>();
    grpcService.commitShuffleTask(
        RssProtos.ShuffleCommitRequest.newBuilder()
            .setAppId(appId)
            .setShuffleId(shuffleId)
            .setStageAttemptNumber(0)
            .build(),
        oldCommitObserver);
    assertEquals(RssProtos.StatusCode.SUCCESS, oldCommitObserver.value.getStatus());
    assertEquals(1, oldCommitObserver.value.getCommitCount());

    Map<Integer, long[]> stage1Blocks = Maps.newHashMap();
    stage1Blocks.put(0, new long[] {layout.getBlockId(0, 0, 1)});
    assertEquals(
        1,
        shuffleTaskManager.addFinishedBlockIds(
            appId, shuffleId, stage1Blocks, 3, true, 1));

    CapturingStreamObserver<RssProtos.ShuffleCommitResponse> currentCommitObserver =
        new CapturingStreamObserver<>();
    grpcService.commitShuffleTask(
        RssProtos.ShuffleCommitRequest.newBuilder()
            .setAppId(appId)
            .setShuffleId(shuffleId)
            .setStageAttemptNumber(1)
            .build(),
        currentCommitObserver);

    assertNull(currentCommitObserver.error);
    assertTrue(currentCommitObserver.completed);
    assertEquals(RssProtos.StatusCode.SUCCESS, currentCommitObserver.value.getStatus());
    assertEquals(1, currentCommitObserver.value.getCommitCount());
  }

  @Test
  public void finishShuffleShouldIgnoreStaleStageAttempt() throws Exception {
    ShuffleServerConf conf = constructServerConfWithLocalfile();
    shuffleServer = new ShuffleServer(conf);
    ShuffleTaskManager shuffleTaskManager = shuffleServer.getShuffleTaskManager();
    ShuffleServerGrpcService grpcService = new ShuffleServerGrpcService(shuffleServer);
    String appId = "testFinishShuffleShouldIgnoreStaleStageAttempt";
    int shuffleId = 1;
    BlockIdLayout layout = BlockIdLayout.DEFAULT;

    shuffleTaskManager.registerShuffle(
        appId,
        shuffleId,
        Lists.newArrayList(new PartitionRange(0, 0)),
        new RemoteStorageInfo(tempDir1.getAbsolutePath()),
        StringUtils.EMPTY);

    Map<Integer, long[]> stage1Blocks = Maps.newHashMap();
    stage1Blocks.put(0, new long[] {layout.getBlockId(0, 0, 1)});
    assertEquals(
        1,
        shuffleTaskManager.addFinishedBlockIds(
            appId, shuffleId, stage1Blocks, 3, true, 1));

    CapturingStreamObserver<RssProtos.FinishShuffleResponse> staleObserver =
        new CapturingStreamObserver<>();
    grpcService.finishShuffle(
        RssProtos.FinishShuffleRequest.newBuilder()
            .setAppId(appId)
            .setShuffleId(shuffleId)
            .setStageAttemptNumber(0)
            .build(),
        staleObserver);

    assertNull(staleObserver.error);
    assertTrue(staleObserver.completed);
    assertEquals(RssProtos.StatusCode.STAGE_RETRY_IGNORE, staleObserver.value.getStatus());
    assertTrue(staleObserver.value.hasStageAttemptAccepted());
    assertFalse(staleObserver.value.getStageAttemptAccepted());
  }

  @Test
  public void startSortMergeShouldRejectStaleAndMissingRegistrationAtRpcLayer() throws Exception {
    ShuffleServerConf conf = constructServerConfWithLocalfile();
    conf.set(ShuffleServerConf.SERVER_MERGE_ENABLE, true);
    conf.set(ShuffleServerConf.SERVER_SHUFFLE_BUFFER_TYPE, ShuffleBufferType.SKIP_LIST);
    shuffleServer = new ShuffleServer(conf);
    ShuffleTaskManager shuffleTaskManager = shuffleServer.getShuffleTaskManager();
    ShuffleServerGrpcService grpcService = new ShuffleServerGrpcService(shuffleServer);
    String appId = "testStartSortMergeShouldRejectStaleAndMissingRegistrationAtRpcLayer";
    int shuffleId = 1;
    int partitionId = 0;
    List<PartitionRange> partitionRanges = Lists.newArrayList(new PartitionRange(partitionId, partitionId));
    RemoteStorageInfo remoteStorageInfo = new RemoteStorageInfo(tempDir1.getAbsolutePath());
    ByteString uniqueBlocks =
        ByteString.copyFrom(RssUtils.serializeBitMap(Roaring64NavigableMap.bitmapOf(1L)));

    shuffleTaskManager.registerShuffle(
        appId, shuffleId, partitionRanges, remoteStorageInfo, StringUtils.EMPTY);
    shuffleServer
        .getShuffleMergeManager()
        .registerShuffle(
            appId,
            shuffleId,
            RssProtos.MergeContext.newBuilder()
                .setKeyClass(String.class.getName())
                .setValueClass(Integer.class.getName())
                .setMergedBlockSize(-1)
                .setMergeClassLoader("")
                .build());

    CapturingStreamObserver<RssProtos.StartSortMergeResponse> currentObserver =
        new CapturingStreamObserver<>();
    grpcService.startSortMerge(
        RssProtos.StartSortMergeRequest.newBuilder()
            .setAppId(appId)
            .setShuffleId(shuffleId)
            .setPartitionId(partitionId)
            .setStageAttemptNumber(1)
            .setUniqueBlocksBitmap(uniqueBlocks)
            .build(),
        currentObserver);

    assertNull(currentObserver.error);
    assertTrue(currentObserver.completed);
    assertEquals(RssProtos.StatusCode.SUCCESS, currentObserver.value.getStatus());
    assertTrue(currentObserver.value.hasStageAttemptAccepted());
    assertTrue(currentObserver.value.getStageAttemptAccepted());

    CapturingStreamObserver<RssProtos.StartSortMergeResponse> staleObserver =
        new CapturingStreamObserver<>();
    grpcService.startSortMerge(
        RssProtos.StartSortMergeRequest.newBuilder()
            .setAppId(appId)
            .setShuffleId(shuffleId)
            .setPartitionId(partitionId)
            .setStageAttemptNumber(0)
            .setUniqueBlocksBitmap(uniqueBlocks)
            .build(),
        staleObserver);

    assertNull(staleObserver.error);
    assertTrue(staleObserver.completed);
    assertEquals(RssProtos.StatusCode.STAGE_RETRY_IGNORE, staleObserver.value.getStatus());
    assertTrue(staleObserver.value.hasStageAttemptAccepted());
    assertFalse(staleObserver.value.getStageAttemptAccepted());

    CapturingStreamObserver<RssProtos.StartSortMergeResponse> missingObserver =
        new CapturingStreamObserver<>();
    grpcService.startSortMerge(
        RssProtos.StartSortMergeRequest.newBuilder()
            .setAppId(appId)
            .setShuffleId(shuffleId + 1)
            .setPartitionId(partitionId)
            .setStageAttemptNumber(0)
            .setUniqueBlocksBitmap(uniqueBlocks)
            .build(),
        missingObserver);

    assertNull(missingObserver.error);
    assertTrue(missingObserver.completed);
    assertEquals(RssProtos.StatusCode.NO_REGISTER, missingObserver.value.getStatus());
    assertTrue(missingObserver.value.hasStageAttemptAccepted());
    assertFalse(missingObserver.value.getStageAttemptAccepted());
  }

  @Test
  public void sendShuffleDataShouldIgnoreStaleStageAttempt() throws Exception {
    ShuffleServerConf conf = constructServerConfWithLocalfile();
    shuffleServer = new ShuffleServer(conf);
    ShuffleTaskManager shuffleTaskManager = shuffleServer.getShuffleTaskManager();
    ShuffleServerGrpcService grpcService = new ShuffleServerGrpcService(shuffleServer);
    String appId = "testSendShuffleDataShouldIgnoreStaleStageAttempt";
    int shuffleId = 1;

    shuffleTaskManager.registerShuffle(
        appId,
        shuffleId,
        Lists.newArrayList(new PartitionRange(0, 0)),
        new RemoteStorageInfo(tempDir1.getAbsolutePath()),
        StringUtils.EMPTY);

    Map<Integer, long[]> stage1Blocks = Maps.newHashMap();
    stage1Blocks.put(0, new long[] {BlockIdLayout.DEFAULT.getBlockId(0, 0, 1)});
    assertEquals(
        1,
        shuffleTaskManager.addFinishedBlockIds(
            appId, shuffleId, stage1Blocks, 3, true, 1));
    long currentPartitionDataSize =
        shuffleTaskManager.getPartitionDataSize(appId, shuffleId, 0);
    long currentUsedMemory = shuffleServer.getShuffleBufferManager().getUsedMemory();
    assertEquals(0, shuffleServer.getShuffleBufferManager().getPreAllocatedSize());

    CapturingStreamObserver<RssProtos.SendShuffleDataResponse> staleObserver =
        new CapturingStreamObserver<>();
    grpcService.sendShuffleData(
        createSendShuffleDataRequest(
            appId, shuffleId, shuffleTaskManager.requireBuffer(1), 0, 12L),
        staleObserver);

    assertNull(staleObserver.error);
    assertTrue(staleObserver.completed);
    assertEquals(RssProtos.StatusCode.STAGE_RETRY_IGNORE, staleObserver.value.getStatus());
    assertEquals(1, shuffleTaskManager.getLatestStageAttemptNumber(appId, shuffleId));
    assertEquals(0, shuffleTaskManager.getCachedBlockCount(appId, shuffleId));
    assertEquals(currentPartitionDataSize, shuffleTaskManager.getPartitionDataSize(appId, shuffleId, 0));
    assertEquals(currentUsedMemory, shuffleServer.getShuffleBufferManager().getUsedMemory());
    assertEquals(0, shuffleServer.getShuffleBufferManager().getPreAllocatedSize());
  }

  private RssProtos.SendShuffleDataRequest createSendShuffleDataRequest(
      String appId, int shuffleId, long requireBufferId, int stageAttemptNumber, long blockId) {
    return RssProtos.SendShuffleDataRequest.newBuilder()
        .setAppId(appId)
        .setShuffleId(shuffleId)
        .setRequireBufferId(requireBufferId)
        .setStageAttemptNumber(stageAttemptNumber)
        .addShuffleData(
            RssProtos.ShuffleData.newBuilder()
                .setPartitionId(0)
                .addBlock(
                    RssProtos.ShuffleBlock.newBuilder()
                        .setBlockId(blockId)
                        .setLength(1)
                        .setUncompressLength(1)
                        .setCrc(0)
                        .setTaskAttemptId(blockId)
                        .setData(ByteString.copyFrom(new byte[] {1}))
                        .build())
                .build())
        .build();
  }

  @Test
  public void checkAndClearLeakShuffleDataTest(@TempDir File tempDir) throws Exception {
    final String appId = "clearLocalTest_appId";

    ShuffleServerConf conf = new ShuffleServerConf();
    final int shuffleId = 1;
    conf.set(ShuffleServerConf.RPC_SERVER_PORT, 1234);
    conf.set(ShuffleServerConf.RSS_COORDINATOR_QUORUM, "localhost:9527");
    conf.set(ShuffleServerConf.JETTY_HTTP_PORT, 12345);
    conf.set(ShuffleServerConf.JETTY_CORE_POOL_SIZE, 64);
    conf.set(ShuffleServerConf.SERVER_BUFFER_CAPACITY, 64L);
    conf.set(ShuffleServerConf.SERVER_MEMORY_SHUFFLE_HIGHWATERMARK_PERCENTAGE, 50.0);
    conf.set(ShuffleServerConf.SERVER_MEMORY_SHUFFLE_LOWWATERMARK_PERCENTAGE, 0.0);
    conf.set(ShuffleServerConf.SERVER_COMMIT_TIMEOUT, 10000L);
    conf.set(ShuffleServerConf.SERVER_APP_EXPIRED_WITHOUT_HEARTBEAT, 2000L);
    conf.set(ShuffleServerConf.HEALTH_CHECK_ENABLE, false);
    conf.set(ShuffleServerConf.RSS_STORAGE_BASE_PATH, Arrays.asList(tempDir.getAbsolutePath()));
    conf.setString(ShuffleServerConf.RSS_STORAGE_TYPE.key(), StorageType.LOCALFILE.name());
    conf.set(ShuffleServerConf.RSS_TEST_MODE_ENABLE, true);
    conf.setLong(ShuffleServerConf.DISK_CAPACITY, 1024L * 1024L * 1024L);
    // make sure not to check leak shuffle data automatically
    conf.setLong(ShuffleServerConf.SERVER_LEAK_SHUFFLE_DATA_CHECK_INTERVAL, 600 * 1000L);

    shuffleServer = new ShuffleServer(conf);
    ShuffleTaskManager shuffleTaskManager = shuffleServer.getShuffleTaskManager();
    shuffleTaskManager.registerShuffle(
        appId,
        shuffleId,
        Lists.newArrayList(new PartitionRange(0, 1)),
        RemoteStorageInfo.EMPTY_REMOTE_STORAGE,
        StringUtils.EMPTY);

    shuffleTaskManager.refreshAppId(appId);
    assertEquals(1, shuffleTaskManager.getAppIds().size());

    // make sure shuffle data flush to disk
    int retry = 0;
    while (retry < 5) {
      Thread.sleep(1000);
      ShufflePartitionedData shuffleData = createPartitionedData(1, 1, 48);
      shuffleTaskManager.cacheShuffleData(appId, shuffleId, false, shuffleData);
      shuffleTaskManager.updateCachedBlockIds(appId, shuffleId, shuffleData);
      shuffleTaskManager.refreshAppId(appId);
      shuffleTaskManager.checkResourceStatus();

      retry++;
    }

    StorageManager storageManager = shuffleServer.getStorageManager();
    assertTrue(storageManager instanceof LocalStorageManager);
    LocalStorageManager localStorageManager = (LocalStorageManager) storageManager;
    // parse appIds from storage
    Set<String> appIdsOnDisk = getAppIdsOnDisk(localStorageManager);
    assertEquals(appIdsOnDisk.size(), shuffleTaskManager.getAppIds().size());
    assertTrue(appIdsOnDisk.contains(appId));

    // make sure heartbeat timeout and resources are removed
    Awaitility.await()
        .timeout(10, TimeUnit.SECONDS)
        .until(() -> shuffleTaskManager.getAppIds().size() == 0);

    // Create the hidden dir to simulate LocalStorageChecker's check
    String storageDir = tempDir.getAbsolutePath();
    File hiddenFile = new File(storageDir + "/" + LocalStorageChecker.CHECKER_DIR_NAME);
    hiddenFile.mkdir();

    Awaitility.await()
        .timeout(10, TimeUnit.SECONDS)
        .until(() -> !getAppIdsOnDisk(localStorageManager).contains(appId));
    assertFalse(appIdsOnDisk.contains(LocalStorageChecker.CHECKER_DIR_NAME));

    // mock leak shuffle data
    File file = new File(tempDir, appId);
    assertFalse(file.exists());
    file.mkdir();
    assertTrue(file.exists());

    // execute checkLeakShuffleData
    shuffleTaskManager.checkLeakShuffleData();
    assertFalse(file.exists());
    assertTrue(hiddenFile.exists());
  }

  private Set<String> getAppIdsOnDisk(LocalStorageManager localStorageManager) {
    Set<String> appIdsOnDisk = new HashSet<>();

    List<LocalStorage> storages = localStorageManager.getStorages();
    for (LocalStorage storage : storages) {
      appIdsOnDisk.addAll(storage.getAppIds());
    }

    return appIdsOnDisk;
  }

  private void waitForFlush(
      ShuffleFlushManager shuffleFlushManager, String appId, int shuffleId, int expectedBlockNum)
      throws Exception {
    int retry = 0;
    while (true) {
      // remove flushed eventId to test timeout in commit
      if (shuffleFlushManager.getCommittedBlockCount(appId, shuffleId) == expectedBlockNum) {
        break;
      }
      Thread.sleep(1000);
      retry++;
      if (retry > 5) {
        fail("Timeout to flush data");
      }
    }
  }

  private ShufflePartitionedData createPartitionedData(
      int partitionId, int blockNum, int dataLength) {
    ShufflePartitionedBlock[] blocks = createBlock(blockNum, dataLength);
    return new ShufflePartitionedData(partitionId, blocks);
  }

  private ShufflePartitionedBlock[] createBlock(int num, int length) {
    ShufflePartitionedBlock[] blocks = new ShufflePartitionedBlock[num];
    for (int i = 0; i < num; i++) {
      byte[] buf = new byte[length];
      new Random().nextBytes(buf);
      blocks[i] =
          new ShufflePartitionedBlock(
              length, length, ChecksumUtils.getCrc32(buf), ATOMIC_INT.incrementAndGet(), 0, buf);
    }
    return blocks;
  }

  private void validate(
      String appId,
      int shuffleId,
      int partitionId,
      List<ShufflePartitionedBlock> blocks,
      String basePath) {
    Roaring64NavigableMap expectBlockIds = Roaring64NavigableMap.bitmapOf();
    Set<Long> processBlockIds = ConcurrentHashMap.newKeySet();
    Set<Long> remainIds = Sets.newHashSet();
    for (ShufflePartitionedBlock spb : blocks) {
      expectBlockIds.addLong(spb.getBlockId());
      remainIds.add(spb.getBlockId());
    }
    HadoopClientReadHandler handler =
        new HadoopClientReadHandler(
            appId,
            shuffleId,
            partitionId,
            100,
            1,
            10,
            1000,
            expectBlockIds,
            processBlockIds,
            basePath,
            new Configuration());

    ShuffleDataResult sdr = handler.readShuffleData();
    List<BufferSegment> bufferSegments = sdr.getBufferSegments();
    int matchNum = 0;
    for (ShufflePartitionedBlock block : blocks) {
      for (BufferSegment bs : bufferSegments) {
        if (bs.getBlockId() == block.getBlockId()) {
          assertEquals(block.getDataLength(), bs.getLength());
          assertEquals(block.getCrc(), bs.getCrc());
          matchNum++;
          break;
        }
      }
    }
    assertEquals(blocks.size(), matchNum);
  }

  @Test
  public void testGetMaxConcurrencyWriting() {
    ShuffleServerConf conf = new ShuffleServerConf();
    conf.set(SERVER_MAX_CONCURRENCY_OF_ONE_PARTITION, 10);
    conf.set(CLIENT_MAX_CONCURRENCY_LIMITATION_OF_ONE_PARTITION, 30);

    // case1: client max concurrency is <= 0
    assertEquals(10, ShuffleTaskManager.getMaxConcurrencyWriting(-1, conf));

    // case2: client max concurrency is 24
    assertEquals(24, ShuffleTaskManager.getMaxConcurrencyWriting(24, conf));

    // case3: client max concurrency exceed 30
    assertEquals(30, ShuffleTaskManager.getMaxConcurrencyWriting(40, conf));
  }

  @Timeout(10)
  @Test
  public void testStorageRemoveResourceHang(@TempDir File tmpDir) throws Exception {
    String confFile = ClassLoader.getSystemResource("server.conf").getFile();
    ShuffleServerConf conf = new ShuffleServerConf(confFile);
    final String storageBasePath = tmpDir.getAbsolutePath() + "rss/testStorageRemoveResourceHang";
    conf.set(RssBaseConf.RSS_STORAGE_TYPE, MEMORY_LOCALFILE);
    conf.set(ShuffleServerConf.RSS_TEST_MODE_ENABLE, true);
    conf.set(ShuffleServerConf.RPC_SERVER_PORT, 1234);
    conf.set(ShuffleServerConf.RSS_COORDINATOR_QUORUM, "localhost:9527");
    conf.set(ShuffleServerConf.STORAGE_REMOVE_RESOURCE_OPERATION_TIMEOUT_SEC, 2L);

    shuffleServer = new ShuffleServer(conf);
    ShuffleTaskManager shuffleTaskManager = shuffleServer.getShuffleTaskManager();

    String appId = "appId1";
    shuffleTaskManager.registerShuffle(
        appId,
        1,
        Lists.newArrayList(new PartitionRange(0, 1)),
        new RemoteStorageInfo(storageBasePath, Maps.newHashMap()),
        StringUtils.EMPTY);
    shuffleTaskManager.refreshAppId(appId);
    assertEquals(1, shuffleTaskManager.getAppIds().size());

    ShufflePartitionedData partitionedData0 = createPartitionedData(1, 1, 35);
    shuffleTaskManager.requireBuffer(35);
    shuffleTaskManager.cacheShuffleData(appId, 0, false, partitionedData0);
    shuffleTaskManager.updateCachedBlockIds(appId, 0, partitionedData0);
    shuffleTaskManager.refreshAppId(appId);
    shuffleTaskManager.checkResourceStatus();
    assertEquals(1, shuffleTaskManager.getAppIds().size());

    // get the underlying localfile storage manager to simulate hang
    LocalStorageManager storageManager = (LocalStorageManager) shuffleServer.getStorageManager();
    storageManager = spy(storageManager);
    doAnswer(
            x -> {
              Thread.sleep(100000);
              return null;
            })
        .when(storageManager)
        .removeResources(any(PurgeEvent.class));
    shuffleTaskManager.setStorageManager(storageManager);

    assertThrows(RssException.class, () -> shuffleTaskManager.removeResources(appId, false));
  }

  @Test
  public void testRegisterShuffleAfterAppIsExpired() throws Exception {
    String confFile = ClassLoader.getSystemResource("server.conf").getFile();
    ShuffleServerConf conf = new ShuffleServerConf(confFile);
    final String storageBasePath = HDFS_URI + "rss/testRegisterShuffleAfterAppIsExpired";
    conf.set(ShuffleServerConf.RSS_TEST_MODE_ENABLE, true);
    conf.set(ShuffleServerConf.RPC_SERVER_PORT, 1234);
    conf.set(ShuffleServerConf.RSS_COORDINATOR_QUORUM, "localhost:9527");

    shuffleServer = new ShuffleServer(conf);
    ShuffleTaskManager shuffleTaskManager = shuffleServer.getShuffleTaskManager();

    String appId = "appId1";
    shuffleTaskManager.registerShuffle(
        appId,
        1,
        Lists.newArrayList(new PartitionRange(0, 1)),
        new RemoteStorageInfo(storageBasePath, Maps.newHashMap()),
        StringUtils.EMPTY);
    shuffleTaskManager.refreshAppId(appId);
    assertEquals(1, shuffleTaskManager.getAppIds().size());

    ShufflePartitionedData partitionedData0 = createPartitionedData(1, 1, 35);
    shuffleTaskManager.requireBuffer(35);
    shuffleTaskManager.cacheShuffleData(appId, 0, false, partitionedData0);
    shuffleTaskManager.updateCachedBlockIds(appId, 0, partitionedData0);
    shuffleTaskManager.refreshAppId(appId);
    shuffleTaskManager.checkResourceStatus();
    assertEquals(1, shuffleTaskManager.getAppIds().size());

    // App is expired due to no heartbeat, so it was added to expired queue and will be removed
    // resource soon.
    Thread thread =
        new Thread(
            () -> {
              try {
                Thread.sleep(1000);
                shuffleTaskManager.removeResources(appId, true);
              } catch (InterruptedException e) {
                throw new RssException(e);
              }
            });
    thread.start();

    // At this moment, this app re-registers shuffle.
    shuffleTaskManager.registerShuffle(
        appId,
        2,
        Lists.newArrayList(new PartitionRange(0, 1)),
        new RemoteStorageInfo(storageBasePath, Maps.newHashMap()),
        StringUtils.EMPTY);
    Thread.sleep(2000);

    // The NO_REGISTER status code should not appear.
    assertTrue(
        shuffleTaskManager.requireBuffer(appId, 2, Arrays.asList(1), Arrays.asList(35), 35) != -4);
    shuffleTaskManager.removeResources(appId, false);
  }
}
