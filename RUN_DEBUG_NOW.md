# 立即运行调试

## 🚀 现在执行

```bash
cd ~/workspaces/OpenMetadata

# 清除缓存（重要！）
find ingestion/src -name "*.pyc" -delete
find ingestion/src -name "__pycache__" -exec rm -rf {} + 2>/dev/null

# 运行测试，只看调试输出
metadata ingest -c ingestion/tests/unit/metadata/ingestion/owner_config_tests/test-03-multiple-users.yaml 2>&1 | grep "🔍"
```

## 📊 现在会看到的输出

### 场景 1: ownerConfig 没有配置（配置解析失败）

```
🔍 [GET_DB_OWNER] database=finance_db, has_ownerConfig=False
🔍 [DB_CHECK] database=finance_db, owner_ref=None, has_root=None
🔍 [DB_NO_OWNER] database=finance_db, clearing context
```

**说明**: `ownerConfig` 没有被正确解析或传递。

**原因**: 可能是 Pydantic 模型生成问题，需要重新生成。

---

### 场景 2: ownerConfig 有，但 owner_ref 是 None（没找到 owner）

```
🔍 [GET_DB_OWNER] database=finance_db, has_ownerConfig=True
🔍 [GET_DB_OWNER] owner_ref=None, has_root=None
🔍 [DB_CHECK] database=finance_db, owner_ref=None, has_root=None
🔍 [DB_NO_OWNER] database=finance_db, clearing context
```

**说明**: 配置存在，但没有匹配到 finance_db 的 owner。

**原因**: 
- FQN 匹配问题
- 配置中的 database 名字不对
- resolve_owner 函数返回了 None

---

### 场景 3: 正常（应该看到）

```
🔍 [GET_DB_OWNER] database=finance_db, has_ownerConfig=True
🔍 [GET_DB_OWNER] owner_ref=EntityReferenceList(...), has_root=[EntityReference(...), EntityReference(...)]
🔍 [DB_CHECK] database=finance_db, owner_ref=EntityReferenceList(...), has_root=[...]
🔍 [STORE_DB] database=finance_db, owner_names=['alice', 'bob'], storing=['alice', 'bob'], type=<class 'list'>
```

**说明**: 一切正常！

---

## 🔍 请告诉我输出

运行后，请把所有 `🔍` 开头的输出都告诉我，特别是：

1. `has_ownerConfig` 是 True 还是 False？
2. `owner_ref` 是什么？
3. 是否看到 `STORE_DB` 或 `DB_NO_OWNER`？

这样我们就能知道问题在哪里了！
