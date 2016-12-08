/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicy;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.protocol.LocatedStripedBlock;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.datanode.DataNodeTestUtils;
import org.apache.hadoop.hdfs.server.datanode.SimulatedFSDataset;
import org.apache.hadoop.hdfs.server.namenode.ErasureCodingPolicyManager;
import org.apache.hadoop.hdfs.util.StripedBlockUtil;
import org.apache.hadoop.io.erasurecode.CodecUtil;
import org.apache.hadoop.io.erasurecode.ErasureCodeNative;
import org.apache.hadoop.io.erasurecode.ErasureCoderOptions;
import org.apache.hadoop.io.erasurecode.rawcoder.NativeRSRawErasureCoderFactory;
import org.apache.hadoop.io.erasurecode.rawcoder.RawErasureDecoder;
import org.apache.hadoop.util.Shell;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.io.File;
import java.io.PrintWriter;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Paths;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.IO_FILE_BUFFER_SIZE_DEFAULT;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.IO_FILE_BUFFER_SIZE_KEY;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestDFSStripedInputStreamGDA {

  public static final Log LOG = LogFactory.getLog(TestDFSStripedInputStream.class);

  private MiniDFSCluster cluster;
  private Configuration conf = new Configuration();
  private DistributedFileSystem fs;
  private final Path dirPath = new Path("/striped");
  private Path filePath = new Path(dirPath, "file");
  private final ErasureCodingPolicy ecPolicy =
      ErasureCodingPolicyManager.getSystemDefaultPolicy();
  private final short DATA_BLK_NUM = StripedFileTestUtil.NUM_DATA_BLOCKS;
  private final short PARITY_BLK_NUM = StripedFileTestUtil.NUM_PARITY_BLOCKS;
  private final int CELLSIZE = StripedFileTestUtil.BLOCK_STRIPED_CELL_SIZE;
  private final int NUM_STRIPE_PER_BLOCK = 2;
  private final int INTERNAL_BLOCK_SIZE = NUM_STRIPE_PER_BLOCK * CELLSIZE;
  private final int BLOCK_GROUP_SIZE =  DATA_BLK_NUM * INTERNAL_BLOCK_SIZE;

  private final int remoteDNIdx = DATA_BLK_NUM - 1;

  @Rule
  public Timeout globalTimeout = new Timeout(300000);

  public void createLinkCostScript(File src) throws IOException {
    // Populate link costs dictionary. This will be used to generate our script
    Map<String, Map<String, Integer>> linkCosts =
        new HashMap<String, Map<String, Integer>>();
    linkCosts.put("/localRack", new HashMap<String, Integer>());
    Map<String, Integer> costsLocal = linkCosts.get("/localRack");
    linkCosts.put("/remoteRack", new HashMap<String, Integer>());
    Map<String, Integer> costsRemote = linkCosts.get("/remoteRack");

    // Set remote costs such that they are much higher than parity computation
    // costs.
    costsLocal.put("/localRack", 0);
    costsLocal.put("/remoteRack", 1000);
    costsRemote.put("/localRack", 1000);
    costsRemote.put("/remoteRack", 0);

    PrintWriter writer = new PrintWriter(src);
    writer.println("#!/bin/bash");
    try {
      for (String rack1 : linkCosts.keySet()) {
        Map<String, Integer> otherRacks = linkCosts.get(rack1);
        for (String rack2 : otherRacks.keySet()) {
          int cost = otherRacks.get(rack2);
          writer.format(
              "if [[ \"$1\" == \"%s\" && \"$2\" == \"%s\" ]]; then echo \"%d\"; fi\n",
              rack1, rack2, cost);
        }
      }
    } finally {
      writer.close();
    }
  }

  @Before
  public void setup() throws IOException, URISyntaxException {
    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, INTERNAL_BLOCK_SIZE);
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_REPLICATION_MAX_STREAMS_KEY, 0);
    conf.setInt(DFSConfigKeys.DFS_CLIENT_PARITY_COMPUTATION_COST_KEY, 100);
    if (ErasureCodeNative.isNativeCodeLoaded()) {
      conf.set(
          CommonConfigurationKeys.IO_ERASURECODE_CODEC_RS_DEFAULT_RAWCODER_KEY,
          NativeRSRawErasureCoderFactory.class.getCanonicalName());
    }
    final ArrayList<String> rackList = new ArrayList<String>();
    final ArrayList<String> hostList = new ArrayList<String>();
    // Add 2 racks. One rack will have one node, and the other will have
    // DATA_BLK_NUM + PARITY_BLK_NUM - 1 nodes.
    for (int i = 0; i < DATA_BLK_NUM + PARITY_BLK_NUM; i++) {
      if (i == remoteDNIdx) {
        // Node will be in remote rack
        rackList.add("/remoteRack");
      } else {
        rackList.add("/localRack");
      }
      hostList.add("/host" + i);
    }

    // Setup link cost script.
    String scriptFileName = "/" +
        Shell.appendScriptExtension("custom-link-script");
    assertEquals(true, scriptFileName != null && !scriptFileName.isEmpty());
    URL shellScript = getClass().getResource(scriptFileName);
    java.nio.file.Path resourcePath = Paths.get(shellScript.toURI());
    FileUtil.setExecutable(resourcePath.toFile(), true);
    FileUtil.setWritable(resourcePath.toFile(), true);

    // Manually create the script with link cost logic
    createLinkCostScript(resourcePath.toFile());
    conf.set(DFSConfigKeys.NET_LINK_SCRIPT_FILE_NAME_KEY,
      resourcePath.toString());

    SimulatedFSDataset.setFactory(conf);
    cluster = new MiniDFSCluster.Builder(conf)
        .numDataNodes(DATA_BLK_NUM + PARITY_BLK_NUM)
        .racks(rackList.toArray(new String[rackList.size()]))
        .hosts(hostList.toArray(new String[hostList.size()]))
        .build();
    cluster.waitActive();
    for (DataNode dn : cluster.getDataNodes()) {
      DataNodeTestUtils.setHeartbeatsDisabledForTests(dn, true);
    }
    fs = cluster.getFileSystem();
    fs.mkdirs(dirPath);
    fs.getClient().setErasureCodingPolicy(dirPath.toString(), null);
  }

  @After
  public void tearDown() {
    if (cluster != null) {
      cluster.shutdown();
      cluster = null;
    }
  }

  /** Test is copied from TestDFSStripedInputStream.testPreadWithDNFailure.
   * Instead of a failed datanode, we have a remote datanode. The rest of the
   * logic remains the same. Our code should work the exact same way.
   */
  @Test
  public void testPreadWithDNRemote() throws Exception {
    final int numBlocks = 4;
    DFSTestUtil.createStripedFile(cluster, filePath, null, numBlocks,
        NUM_STRIPE_PER_BLOCK, false);
    LocatedBlocks lbs = fs.getClient().namenode.getBlockLocations(
        filePath.toString(), 0, BLOCK_GROUP_SIZE);

    assert lbs.get(0) instanceof LocatedStripedBlock;
    LocatedStripedBlock bg = (LocatedStripedBlock)(lbs.get(0));
    for (int i = 0; i < DATA_BLK_NUM + PARITY_BLK_NUM; i++) {
      Block blk = new Block(bg.getBlock().getBlockId() + i,
          NUM_STRIPE_PER_BLOCK * CELLSIZE,
          bg.getBlock().getGenerationStamp());
      blk.setGenerationStamp(bg.getBlock().getGenerationStamp());
      cluster.injectBlocks(i, Arrays.asList(blk),
          bg.getBlock().getBlockPoolId());
    }
    DFSStripedInputStream in =
        new DFSStripedInputStream(fs.getClient(), filePath.toString(), false,
            ecPolicy, null);
    int readSize = BLOCK_GROUP_SIZE;
    byte[] readBuffer = new byte[readSize];
    byte[] expected = new byte[readSize];

    /** A variation of {@link DFSTestUtil#fillExpectedBuf} for striped blocks */
    for (int i = 0; i < NUM_STRIPE_PER_BLOCK; i++) {
      for (int j = 0; j < DATA_BLK_NUM; j++) {
        for (int k = 0; k < CELLSIZE; k++) {
          int posInBlk = i * CELLSIZE + k;
          int posInFile = i * CELLSIZE * DATA_BLK_NUM + j * CELLSIZE + k;
          expected[posInFile] = SimulatedFSDataset.simulatedByte(
              new Block(bg.getBlock().getBlockId() + j), posInBlk);
        }
      }
    }

    ErasureCoderOptions coderOptions = new ErasureCoderOptions(
        DATA_BLK_NUM, PARITY_BLK_NUM);
    RawErasureDecoder rawDecoder = CodecUtil.createRawDecoder(conf,
        ecPolicy.getCodecName(), coderOptions);

    // Update the expected content for decoded data.
    // It will seem like remoteDNIdx is a failed node.
    int[] missingBlkIdx = new int[PARITY_BLK_NUM];
    for (int i = 0; i < missingBlkIdx.length; i++) {
      if (i == 0) {
        missingBlkIdx[i] = remoteDNIdx;
      } else {
        missingBlkIdx[i] = DATA_BLK_NUM + i;
      }
    }
    for (int i = 0; i < NUM_STRIPE_PER_BLOCK; i++) {
      byte[][] decodeInputs = new byte[DATA_BLK_NUM + PARITY_BLK_NUM][CELLSIZE];
      byte[][] decodeOutputs = new byte[missingBlkIdx.length][CELLSIZE];
      for (int j = 0; j < DATA_BLK_NUM; j++) {
        int posInBuf = i * CELLSIZE * DATA_BLK_NUM + j * CELLSIZE;
        if (j != remoteDNIdx) {
          System.arraycopy(expected, posInBuf, decodeInputs[j], 0, CELLSIZE);
        }
      }
      for (int j = DATA_BLK_NUM; j < DATA_BLK_NUM + PARITY_BLK_NUM; j++) {
        for (int k = 0; k < CELLSIZE; k++) {
          int posInBlk = i * CELLSIZE + k;
          decodeInputs[j][k] = SimulatedFSDataset.simulatedByte(
              new Block(bg.getBlock().getBlockId() + j), posInBlk);
        }
      }
      for (int m : missingBlkIdx) {
        decodeInputs[m] = null;
      }
      rawDecoder.decode(decodeInputs, missingBlkIdx, decodeOutputs);
      int posInBuf = i * CELLSIZE * DATA_BLK_NUM + remoteDNIdx * CELLSIZE;
      System.arraycopy(decodeOutputs[0], 0, expected, posInBuf, CELLSIZE);
    }

    int delta = 10;
    int done = 0;
    // read a small delta, shouldn't trigger decode
    // |cell_0 |
    // |10     |
    done += in.read(0, readBuffer, 0, delta);
    assertEquals(delta, done);
    // both head and trail cells are partial
    // |c_0      |c_1    |c_2 |c_3 |c_4      |c_5         |
    // |256K - 10|missing|256K|256K|256K - 10|not in range|
    done += in.read(delta, readBuffer, delta,
        CELLSIZE * (DATA_BLK_NUM - 1) - 2 * delta);
    assertEquals(CELLSIZE * (DATA_BLK_NUM - 1) - delta, done);
    // read the rest
    done += in.read(done, readBuffer, done, readSize - done);
    assertEquals(readSize, done);
    assertArrayEquals(expected, readBuffer);
  }

}
