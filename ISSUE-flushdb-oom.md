# flushdb 后立即重新灌数据会直接返回 OOM，而非阻塞等待 checkpoint

> 适用仓库：`eloqdata/tx_service`（EloqKV 的 `data_substrate` 子模块）。引擎层 bug，EloqKV 侧只是触发方。

## 现象 / Summary

在一个**单节点、开启持久化存储**的实例上：

1. 灌入大量数据，使内存接近 `node_memory_limit_mb` 上限；
2. 执行 `flushdb`；
3. **立即**重新灌入大量数据。

第 3 步会收到 `OUT_OF_MEMORY`（客户端报错 `Transaction failed due to out of memory.`）。

## 期望行为 / Expected

正常内存满时，写请求应进入内存等待队列（`EnqueueWaitListIfMemoryFull`），**阻塞**直到后台 `ShardCleanCc` 把已 checkpoint 的干净页驱逐、腾出空间后再被唤醒；只要 KV 可写、checkpoint 能推进，就不应直接返回 OOM。

普通单 key 写入的 `abort_if_oom = HoldingRangeReadLock()` 一般为 `false`（`tx_service/src/tx_execution.cpp:4027` 等），也佐证了正常路径应当是阻塞而非中止。

## 实际行为 / Actual

`flushdb` 之后，写请求走了**立即报错**分支而非阻塞分支。

## 根因 / Root cause

`flushdb`（default 库）走 `TruncateTable`，最终调用 `CcShard::CleanCcmPages(..., truncate_table=true)`，把被 truncate 的表的 CcMap 标记为 `ccm_has_full_entries_ = true`：

```cpp
// tx_service/src/cc/cc_shard.cpp:2374-2377 (native) / 2404-2407 (failover)
if (truncate_table) {
    ccm->ccm_has_full_entries_ = true;
}
```

语义：truncate 后该表对应一个全新的空 kv 表，KV 中无数据可拉取，于是引擎认为"全部数据都在内存里"（cache miss = 不存在），并据此**禁用驱逐**（一旦换出某 key，后续读会因"内存没有即不存在"而错误返回 not found）。

而写入路径在内存满、需要新建 `CcEntry` 时，会先检查该标志：

```cpp
// tx_service/include/cc/object_cc_map.h:435-460
if (cce == nullptr) {
    if (txservice_skip_kv || ccm_has_full_entries_) {
        if (req.IsReadOnly()) { ... }
        else {
            hd_res->SetError(CcErrorCode::OUT_OF_MEMORY);  // 立即 OOM
            return true;
        }
    }
    // 否则：挂起等待，待驱逐/ckpt 后唤醒
    shard_->EnqueueWaitListIfMemoryFull(&req);
    return false;
}
```

因此 `flushdb` 后只要内存被打满，就命中第 439 行的立即 OOM 分支，而不是第 459 行的阻塞分支。

## 触发链 / Trigger chain

1. `flushdb` → `TruncateTable` → `CleanCcmPages(truncate_table=true)` → 新表 `ccm_has_full_entries_ = true`，驱逐被禁。
2. 紧接着大量写入，数据全在内存、不可驱逐。
3. 内存撑满 → 命中 `object_cc_map.h:439` 立即 OOM，而非阻塞等 checkpoint。

## 是否自愈 / Self-healing

会自愈，但存在时间窗。`ccm_has_full_entries_` 在生产驱逐路径会被重置回 `false`（当确有有效 key 被驱逐时）：

```cpp
// tx_service/include/cc/template_cc_map.h:11189-11192
if (clean_guard->EvictedValidKeys()) {
    ccm_has_full_entries_ = false;
}
```

即等 checkpointer（默认 `checkpointer_interval ≈ 10s`）把新灌数据刷成干净页后，`ShardCleanCc` 才能驱逐并清掉该标志。问题在于：**flushdb 后立即猛灌，会在第一个 checkpoint 周期完成前就把内存打满**，正好卡在这个窗口直接 OOM。

## 复现步骤 / Repro

```bash
# 单节点，enable_data_store=true，node_memory_limit_mb 设小一点便于复现（如 512）
redis-cli debug ... # 或用 redis-benchmark / 脚本灌到接近内存上限
redis-cli flushdb
# 立即再次灌入大量数据 -> 期望阻塞，实际返回 OOM
```

## 影响 / Impact

`flushdb` 后立即批量重灌（导入、重置后重建数据集等常见场景）会非预期失败，而正常情况下应阻塞等待 checkpoint。

## 建议修复方向 / Suggested fix

理想情况下，truncate 把表标记为 `ccm_has_full_entries_ = true` 只应影响"cache miss 视为不存在、不回 KV 拉取"的读语义；当 **KV 可写且 checkpoint 能推进**时，写入内存满不应直接 OOM，而应允许进入等待队列阻塞、由 checkpoint+驱逐腾出空间后唤醒。可能的方案：

- 写路径在 `ccm_has_full_entries_ == true` 但 KV 存在（`!txservice_skip_kv`）时，先尝试触发 checkpoint 并走等待队列，仅在确实无可驱逐项（如 `ShardCleanCc` 扫到尾仍无法腾空）时才返回 OOM；或
- truncate 后不立即把整表标记为 `ccm_has_full_entries_`，改为依赖现有的 per-entry 状态判断。

## 规避办法 / Workaround

1. `flushdb` 后等一个 checkpoint 周期再灌（或先少量写入触发 ckpt 让标志清掉）。
2. 调小 `checkpointer_interval`，缩短易触发窗口。
3. 灌库时降低速率/批量，给后台 ckpt+驱逐留追赶时间。
4. 调大 `node_memory_limit_mb`。

## 相关代码 / Code references

- `tx_service/src/cc/cc_shard.cpp:2349-2413` — `CleanCcmPages`，truncate 时置 `ccm_has_full_entries_ = true`
- `tx_service/include/cc/object_cc_map.h:435-460` — 写入内存满时的 OOM vs 阻塞分叉
- `tx_service/include/cc/template_cc_map.h:11189-11192` — 驱逐时重置标志
- `tx_service/src/cc/cc_shard.cpp:542-590` — `EnqueueWaitListIfMemoryFull` / `AbortRequestsAfterMemoryFree`
- `tx_service/src/cc/cc_req_misc.cpp:1445-1489` — `ShardCleanCc::Execute`，驱逐与 OOM 中止逻辑
- `tx_service/src/redis_service.cpp:2771-2920`（eloqkv）— `ExecuteFlushDBCommand` 走 `TruncateTable`
