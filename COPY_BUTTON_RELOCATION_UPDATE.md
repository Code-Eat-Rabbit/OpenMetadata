# 复制链接按钮位置调整 - 更新说明

## 变更概述

根据用户反馈，将"复制链接"功能从 **Manage 菜单（⋮）** 移至 **标题旁边的复制按钮** 位置，使其更显眼、更易访问。

---

## 变更前后对比

### 之前的位置 ❌
```
标题 [复制按钮] [关注]          [...Manage]
                              ↓ 点击展开
                        ┌────────────────┐
                        │ 复制名称链接   │
                        │ 复制永久链接   │
                        ├────────────────┤
                        │ Export         │
                        │ Rename         │
                        └────────────────┘
```

### 现在的位置 ✅
```
标题 [复制按钮▼] [关注]          [...Manage]
      ↓ 点击展开
    ┌────────────────┐
    │ 🔗 Copy Name-based Link      │
    │ 📋 Copy Permanent Link       │
    └────────────────┘
```

**优势：**
- ✅ 更显眼 - 直接在标题旁边
- ✅ 更直观 - 符合用户预期（复制按钮就在标题旁边）
- ✅ 更简洁 - Manage 菜单不再臃肿

---

## 修改的文件

### 1. EntityHeaderTitle.component.tsx
**位置：** `openmetadata-ui/src/main/resources/ui/src/components/Entity/EntityHeaderTitle/`

**主要变更：**
- 新增 `entityId` 和 `entityFqn` props
- 将单一复制按钮改为下拉菜单
- 添加两个选项：复制名称链接、复制永久链接
- 如果没有 `entityId` 和 `entityFqn`，则显示原始的单一复制按钮（保持向后兼容）

**关键代码：**
```typescript
// 新增 props
interface EntityHeaderTitleProps {
  // ...
  entityId?: string;
  entityFqn?: string;
}

// 复制逻辑
const handleCopyFqnLink = async () => {
  const fqnUrl = `${window.location.origin}/glossary/${encodeURIComponent(entityFqn)}`;
  await navigator.clipboard.writeText(fqnUrl);
  showSuccessToast(t('message.fqn-link-copied'));
};

const handleCopyPermanentLink = async () => {
  const permanentUrl = `${window.location.origin}/glossary/${entityId}`;
  await navigator.clipboard.writeText(permanentUrl);
  showSuccessToast(t('message.permanent-link-copied'));
};

// 渲染下拉菜单
{entityId && entityFqn ? (
  <Dropdown menu={{ items: [...] }}>
    <Button icon={<Icon component={ShareIcon} />} />
  </Dropdown>
) : (
  <Button onClick={handleShareButtonClick} />  // 原始行为
)}
```

### 2. EntityHeaderTitle.interface.ts
**位置：** `openmetadata-ui/src/main/resources/ui/src/components/Entity/EntityHeaderTitle/`

**变更：**
```typescript
export interface EntityHeaderTitleProps {
  // ... 其他 props
  entityId?: string;         // 新增
  entityFqn?: string;        // 新增
}
```

### 3. EntityHeader.component.tsx
**位置：** `openmetadata-ui/src/main/resources/ui/src/components/Entity/EntityHeader/`

**变更：**
- 更新 Props interface，添加 `id` 字段
- 将 `entityData.id` 和 `entityData.fullyQualifiedName` 传递给 `EntityHeaderTitle`

```typescript
interface Props {
  entityData: {
    // ...
    id?: string;  // 新增
  };
}

// 传递给 EntityHeaderTitle
<EntityHeaderTitle
  entityId={entityData.id}
  entityFqn={entityData.fullyQualifiedName}
  // ... 其他 props
/>
```

### 4. GlossaryHeader.component.tsx
**位置：** `openmetadata-ui/src/main/resources/ui/src/components/Glossary/GlossaryHeader/`

**变更：**
- **移除** `handleCopyFqnLink` 函数
- **移除** `handleCopyPermanentLink` 函数
- **移除** `copyLinkMenuItems` 数组
- **移除** Manage 菜单中的复制链接选项和分隔符
- **清理** 不再使用的导入（`LinkIcon`, `CopyIcon`, `showSuccessToast`）

---

## 技术亮点

### 1. 向后兼容 ✅
```typescript
{entityId && entityFqn ? (
  // 新的下拉菜单（用于词汇表术语）
  <Dropdown>...</Dropdown>
) : (
  // 原始的单一按钮（用于其他实体）
  <Button onClick={handleShareButtonClick} />
)}
```

当 `entityId` 和 `entityFqn` 都存在时，显示下拉菜单；否则保持原始行为。

### 2. 复用现有组件 ✅
- 使用项目自带的 SVG 图标（`link.svg`, `icon-copy.svg`）
- 复用现有的翻译文本（`label.copy-fqn-link`, `label.copy-permanent-link`）
- 沿用 Ant Design 的 `Dropdown` 组件

### 3. 最小化侵入性 ✅
- 只修改 3 个核心组件
- 不影响其他实体类型的复制按钮行为
- 保持代码清晰简洁

---

## UI 效果演示

### 点击复制按钮

```
┌──────────────────────────────────────────┐
│  Glossary Term Name  [📋▼] [⭐ Follow]  │
│                       ↓                   │
│              ┌─────────────────────────┐  │
│              │ 🔗 Copy Name-based Link│  │
│              │ 📋 Copy Permanent Link │  │
│              └─────────────────────────┘  │
└──────────────────────────────────────────┘
```

### 点击后的效果
- ✅ 显示 Toast 提示："名称链接已复制到剪贴板" 或 "永久链接已复制到剪贴板"
- ✅ 下拉菜单自动关闭
- ✅ 链接已在剪贴板中，可直接粘贴

---

## 适用范围

### 支持下拉菜单的实体类型
- ✅ **Glossary Term** - 词汇表术语（主要使用场景）
- ⚠️ 其他实体类型 - 需要传递 `entityId` 和 `entityFqn` 才能启用下拉菜单

### 保持原始行为的实体类型
所有未提供 `entityId` 和 `entityFqn` 的实体类型将继续使用单一复制按钮，复制当前页面 URL。

---

## 测试验证

### 测试步骤

1. **启动应用并访问词汇表术语页面**
   ```bash
   cd /workspace/openmetadata-ui/src/main/resources/ui
   yarn build
   cd /workspace
   ./bin/openmetadata-server-start.sh
   ```

2. **验证复制按钮位置**
   - 打开任意词汇表术语页面
   - 确认标题旁边有复制按钮（📋图标）

3. **验证下拉菜单**
   - 点击复制按钮
   - 应显示两个选项：
     - 🔗 Copy Name-based Link
     - 📋 Copy Permanent Link

4. **验证复制名称链接**
   - 点击 "Copy Name-based Link"
   - 验证 Toast 提示："名称链接已复制到剪贴板"
   - 粘贴到浏览器，验证格式：`/glossary/{fqn}`

5. **验证复制永久链接**
   - 点击 "Copy Permanent Link"
   - 验证 Toast 提示："永久链接已复制到剪贴板"
   - 粘贴到浏览器，验证格式：`/glossary/{uuid}`

6. **验证重命名后的稳定性**
   - 复制术语的永久链接
   - 重命名术语
   - 访问之前复制的链接
   - ✅ 应该仍能正常访问

7. **验证 Manage 菜单**
   - 点击 Manage 按钮（⋮）
   - 确认菜单中**不再有**复制链接相关选项
   - 应直接显示 Export, Rename, Delete 等选项

---

## 兼容性说明

### 浏览器兼容性
- ✅ Chrome 66+
- ✅ Firefox 63+
- ✅ Safari 13.1+
- ✅ Edge 79+

（基于 `navigator.clipboard.writeText` API 支持）

### OpenMetadata 版本
- 适用于当前版本及后续版本
- 不影响现有功能

---

## 回滚方案

如果需要回滚到 Manage 菜单中的实现：

1. **恢复 GlossaryHeader.component.tsx**
   - 恢复 `handleCopyFqnLink` 和 `handleCopyPermanentLink` 函数
   - 恢复 `copyLinkMenuItems` 数组
   - 在 `manageButtonContent` 中添加回链接选项

2. **简化 EntityHeaderTitle.component.tsx**
   - 移除下拉菜单逻辑
   - 恢复单一复制按钮

---

## 相关文档

- [IMPLEMENTATION_SUMMARY.md](/workspace/IMPLEMENTATION_SUMMARY.md) - 完整实施总结
- [ICON_FIX_UPDATE.md](/workspace/ICON_FIX_UPDATE.md) - 图标修复说明

---

## 总结

✅ **按钮位置更合理** - 从 Manage 菜单移至标题旁边  
✅ **用户体验提升** - 更显眼、更易访问  
✅ **代码更简洁** - GlossaryHeader 代码减少，逻辑更清晰  
✅ **向后兼容** - 不影响其他实体类型  
✅ **功能完整** - 两种链接格式都可复制

---

**更新日期：** 2025-10-28  
**状态：** ✅ 已完成，待测试验证
