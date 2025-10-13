# 最终调试测试

## 🎯 现在请运行

```bash
cd ~/workspaces/OpenMetadata

# 清除缓存
find ingestion/src -name "*.pyc" -delete

# 运行测试，只看调试输出
metadata ingest -c ingestion/tests/unit/metadata/ingestion/owner_config_tests/test-03-multiple-users.yaml 2>&1 | grep "🔍" | head -20
```

## 📊 分析输出

### 场景 1: 存储时就是列表，但获取时变成字符串

```
🔍 [STORE_DB] database=finance_db, owner_names=['alice', 'bob'], storing=['alice', 'bob'], type=<class 'list'>
🔍 [GET_SCHEMA] schema=accounting, parent_owner from context=alice, type=<class 'str'>
```

**说明**：Context 在多线程环境下复制时出现问题，列表被转换成了字符串。

**解决方法**：需要检查 TopologyContextManager 的实现，或者改变存储策略。

---

### 场景 2: 存储时就变成了字符串

```
🔍 [STORE_DB] database=finance_db, owner_names=['alice', 'bob'], storing=alice, type=<class 'str'>
🔍 [GET_SCHEMA] schema=accounting, parent_owner from context=alice, type=<class 'str'>
```

**说明**：存储逻辑有问题，`len(database_owner_names) == 1` 的判断不正确。

**解决方法**：检查 `database_owner_names` 的长度判断。

---

### 场景 3: 正常（应该看到的）

```
🔍 [STORE_DB] database=finance_db, owner_names=['alice', 'bob'], storing=['alice', 'bob'], type=<class 'list'>
🔍 [GET_SCHEMA] schema=accounting, parent_owner from context=['alice', 'bob'], type=<class 'list'>
```

**说明**：存储和获取都正常，问题在别处。

---

## 🔧 根据场景采取行动

请把调试输出告诉我，我会根据具体情况给出解决方案！
