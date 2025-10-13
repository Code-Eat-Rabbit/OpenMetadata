#!/bin/bash

# 添加调试输出到关键位置

echo "添加调试输出到关键文件..."

COMMON_DB_FILE="ingestion/src/metadata/ingestion/source/database/common_db_source.py"
OWNER_UTILS_FILE="ingestion/src/metadata/utils/owner_utils.py"

# 1. 在 common_db_source.py 添加调试（database owner存储后）
echo "【1】添加 database owner 调试..."

# 找到第228行（upsert后），插入调试代码
sed -i.bak '228 a\
        # 🔍 DEBUG OUTPUT\
        import sys\
        print(f"🔍 [DB] database_owner_names = {database_owner_names}", file=sys.stderr)\
        print(f"🔍 [DB] database_owner (context) = {database_owner}", file=sys.stderr)\
        print(f"🔍 [DB] type = {type(database_owner).__name__}", file=sys.stderr)
' "$COMMON_DB_FILE"

# 2. 在 common_db_source.py 添加调试（schema owner存储后）
echo "【2】添加 schema owner 调试..."

sed -i '290 a\
        # 🔍 DEBUG OUTPUT\
        import sys\
        print(f"🔍 [SCHEMA] schema_owner_names = {schema_owner_names}", file=sys.stderr)\
        print(f"🔍 [SCHEMA] schema_owner (context) = {schema_owner}", file=sys.stderr)\
        print(f"🔍 [SCHEMA] type = {type(schema_owner).__name__}", file=sys.stderr)
' "$COMMON_DB_FILE"

# 3. 在 owner_utils.py 添加调试（resolve_owner 继承时）
echo "【3】添加 resolve_owner 调试..."

sed -i.bak '117 a\
            # 🔍 DEBUG OUTPUT\
            import sys\
            print(f"🔍 [RESOLVE] entity={entity_name}, parent_owner={parent_owner}", file=sys.stderr)\
            print(f"🔍 [RESOLVE] parent_owner type={type(parent_owner).__name__}", file=sys.stderr)
' "$OWNER_UTILS_FILE"

# 在 _get_owner_refs 调用后添加
sed -i '122 a\
            # 🔍 DEBUG OUTPUT\
            if owner_ref and owner_ref.root:\
                import sys\
                print(f"🔍 [RESOLVE] _get_owner_refs returned {len(owner_ref.root)} owners: {[o.name for o in owner_ref.root]}", file=sys.stderr)
' "$OWNER_UTILS_FILE"

# 4. 在 _get_owner_refs 函数中添加调试
echo "【4】添加 _get_owner_refs 调试..."

sed -i '160 a\
        # 🔍 DEBUG OUTPUT\
        import sys\
        print(f"🔍 [GET_REFS] Input owner_names={owner_names} (type={type(owner_names).__name__})", file=sys.stderr)
' "$OWNER_UTILS_FILE"

sed -i '226 a\
        # 🔍 DEBUG OUTPUT\
        import sys\
        print(f"🔍 [GET_REFS] Returning {len(all_owners) if all_owners else 0} owners: {[o.name for o in all_owners] if all_owners else []}", file=sys.stderr)
' "$OWNER_UTILS_FILE"

echo ""
echo "✅ 调试输出已添加！"
echo ""
echo "备份文件："
echo "  - $COMMON_DB_FILE.bak"
echo "  - $OWNER_UTILS_FILE.bak"
echo ""
echo "现在运行："
echo "  metadata ingest -c test-03-multiple-users.yaml 2>&1 | grep '🔍'"
