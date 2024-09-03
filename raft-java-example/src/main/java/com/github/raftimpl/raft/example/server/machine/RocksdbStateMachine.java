package com.github.raftimpl.raft.example.server.machine;

import com.github.raftimpl.raft.StateMachine;
import com.github.raftimpl.raft.example.server.service.ExampleProto;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.rocksdb.*;

import java.io.File;

/**
 * Description:
 *      使用RocksDB 主要实现核心代码中状态机操作
 * @author Jerry
 * @version 1.0
 * 2024/9/4 18:45
 */
public class RocksdbStateMachine implements StateMachine {

    private static final Logger LOG = LoggerFactory.getLogger(RocksdbStateMachine.class);

    static {
        RocksDB.loadLibrary();
    }

    private RocksDB db;
    private String raftDataDir;

    public RocksdbStateMachine(String raftDataDir) {
        this.raftDataDir = raftDataDir;
    }

    @Override
    public void writeSnapshot(String snapshotDir) {
        Checkpoint checkpoint = Checkpoint.create(db);
        try {
            // 构建一个可打开的 RocksDB 快照
            checkpoint.createCheckpoint(snapshotDir);
        } catch (Exception e) {
            e.printStackTrace();
            LOG.warn("writeSnapshot meet exception, dir={}, msg={}",
                    snapshotDir, e.getMessage());
        }
    }

    @Override
    public void readSnapshot(String snapshotDir) {
        try {
            if (db != null) {
                db.close();
            }
            // 将快照复制到data目录下
            String datadir = raftDataDir + File.separator + "rocksdb_data";
            File dataFile = new File(datadir);
            if (dataFile.exists()) {
                FileUtils.deleteDirectory(dataFile);
            }
            File snapshotFile = new File(snapshotDir);
            if (snapshotFile.exists()) {
                FileUtils.copyDirectory(snapshotFile, dataFile);
            }
            // 打开 rocksdb data dir
            db = createRocksDB();
        }catch (Exception e){
            e.printStackTrace();
            LOG.warn("readSnapshot meet exception, dir={}, msg={}",
                    snapshotDir, e.getMessage());
        }
    }

    @Override
    public void apply(byte[] dataBytes) {
        try {
            ExampleProto.SetRequest request = ExampleProto.SetRequest.parseFrom(dataBytes);
            db.put(request.getKey().getBytes(), request.getValue().getBytes());
        } catch (Exception e) {
            LOG.warn("apply meet exception, msg={}",
                    e.getMessage());
        }
    }

    public ExampleProto.GetResponse get(ExampleProto.GetRequest request) {
        try {
            ExampleProto.GetResponse.Builder responseBuilder = ExampleProto.GetResponse.newBuilder();
            byte[] keyBytes = request.getKey().getBytes();
            byte[] valueBytes = db.get(keyBytes);
            if (valueBytes != null) {
                String value = new String(valueBytes);
                responseBuilder.setValue(value);
            }
            ExampleProto.GetResponse response = responseBuilder.build();
            return response;
        } catch (RocksDBException ex) {
            LOG.warn("read rockdb error, msg={}", ex.getMessage());
            return null;
        }
    }

    public RocksDB createRocksDB() throws RocksDBException {
        Options options = new Options().setCreateIfMissing(true)
                // 内存管理
                .setWriteBufferSize(128 * 1024 * 1024) // MemTable 大小，可以根据写入速率调整
                .setMaxWriteBufferNumber(4) // 最大 MemTable 数量，用于并发写入
                .setMinWriteBufferNumberToMerge(2) // 合并 MemTable 的最小数量
                .setTargetFileSizeBase(256 * 1024 * 1024) // 每个 SSTable 文件的目标大小，根据磁盘 I/O 能力调整
                .setMaxBytesForLevelBase(1024 * 1024 * 1024) // Level 1 的目标大小，根据磁盘空间调整
                .setMaxBytesForLevelMultiplier(10) // 每层的目标大小是前一层的倍数，控制 Compaction 触发条件
                .setLevelCompactionDynamicLevelBytes(true) // 动态调整每层大小

                // WAL 配置
                .setWalDir("/path/to/wal") // WAL 日志路径
                .setWalTtlSeconds(60 * 60 * 24) // WAL 日志保留时间，根据数据安全性需求调整
                .setWalSizeLimitMB(2048) // WAL 日志大小限制，根据磁盘空间调整
                .setUseFsync(false) // 是否在写入时使用 fsync，平衡性能和数据安全

                // Block Cache
                .setTableFormatConfig(new BlockBasedTableConfig()
                        .setBlockSize(16 * 1024) // Block 大小，根据数据特性调整
                        .setPinL0FilterAndIndexBlocksInCache(true) // Pin Level 0 的过滤器和索引块
                        .setCacheIndexAndFilterBlocks(true) // 缓存索引和过滤器块，提高读性能
                        .setBlockCache(new LRUCache(512*1024*1024))) // Block Cache 大小，根据内存资源调整

                // 数据压缩
                .setCompressionType(CompressionType.LZ4_COMPRESSION) // 使用 LZ4 压缩算法，也可以选择其他算法

                // 并发控制
                .setMaxTotalWalSize(2 * 1024 * 1024 * 1024) // WAL 文件总大小限制
                .setMaxOpenFiles(-1) // 允许打开的最大文件数，-1 表示无限制
                .setMaxSequentialSkipInIterations(8) // 优化迭代器性能

                // 其他优化
                .setTableFormatConfig(new BlockBasedTableConfig().setFilterPolicy(new BloomFilter(10, false))); // 使用布隆过滤器减少读操作

        return RocksDB.open(options, raftDataDir);
    }
}
