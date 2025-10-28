# 图标和菜单结构修复 - 更新说明

## 修复内容

### 1. 图标显示问题 ✅

**问题：** 菜单项不显示图标

**原因：** 使用了 Ant Design 的图标组件，但 `ManageButtonItemLabel` 需要项目自带的 SVG React 组件。

**修复：**
```typescript
// 修改前（错误）
import { CopyOutlined, LinkOutlined } from '@ant-design/icons';

// 修改后（正确）
import { ReactComponent as LinkIcon } from '../../../assets/svg/link.svg';
import { ReactComponent as CopyIcon } from '../../../assets/svg/icon-copy.svg';
```

**使用的图标：**
- 🔗 `link.svg` - 用于"复制名称链接"
- 📋 `icon-copy.svg` - 用于"复制永久链接"

---

### 2. 菜单结构调整 ✅

**问题：** 子菜单不显示

**原因：** Ant Design Menu 的 `children` 属性在与 `ManageButtonItemLabel` 组合使用时可能不兼容。

**修复：** 将两个链接选项直接展开到主菜单顶部，用分隔符与其他选项区分。

**修改前的结构（子菜单方式 - 不工作）：**
```typescript
{
  label: '复制链接',
  children: [
    { label: '复制名称链接', ... },
    { label: '复制永久链接', ... }
  ]
}
```

**修改后的结构（平铺方式 - 工作）：**
```typescript
[
  { label: '复制名称链接', ... },
  { label: '复制永久链接', ... },
  { type: 'divider' },
  // 其他菜单项...
]
```

---

### 3. 最终 UI 效果

**Manage 下拉菜单（从上到下）：**

```
┌──────────────────────────────────────────────┐
│ 🔗 Copy Name-based Link                      │
│    Copy link with the term's full name.     │
│    URL is readable and shows hierarchy...    │
├──────────────────────────────────────────────┤
│ 📋 Copy Permanent Link                       │
│    Copy stable ID-based link. URL doesn't   │
│    include name but remains valid...         │
├──────────────────────────────────────────────┤ ← 分隔线
│ 📤 Export (如果是 Glossary)                  │
│ 📥 Import (如果是 Glossary)                  │
│ ✏️  Rename                                    │
│ 🎨 Style (如果是 Term)                       │
│ 🔄 Change Parent (如果是 Term)               │
│ 🗑️  Delete                                    │
└──────────────────────────────────────────────┘
```

---

## 代码变更总结

### 文件：GlossaryHeader.component.tsx

**导入变更：**
```diff
- import Icon, { CopyOutlined, DownOutlined, LinkOutlined } from '@ant-design/icons';
+ import Icon, { DownOutlined } from '@ant-design/icons';
+ import { ReactComponent as LinkIcon } from '../../../assets/svg/link.svg';
+ import { ReactComponent as CopyIcon } from '../../../assets/svg/icon-copy.svg';
```

**图标使用：**
```diff
- icon={LinkOutlined}
+ icon={LinkIcon}

- icon={CopyOutlined}
+ icon={CopyIcon}
```

**菜单结构：**
```diff
  const manageButtonContent: ItemType[] = [
-   {
-     label: t('label.copy-link'),
-     key: 'copy-link-menu',
-     icon: <Icon component={LinkIcon} />,
-     children: copyLinkMenuItems,
-   },
+   ...copyLinkMenuItems,
+   {
+     type: 'divider',
+   },
    ...(isGlossary && importExportPermissions
```

---

## 验证步骤

### 构建前端
```bash
cd /workspace/openmetadata-ui/src/main/resources/ui
yarn build
```

### 启动服务
```bash
cd /workspace
./bin/openmetadata-server-start.sh
```

### 测试点击路径
1. 打开任意词汇表术语页面（如 `/glossary/MyGlossary.MyTerm`）
2. 点击页面右上角的 **Manage** 按钮（三点图标 `⋮`）
3. 查看下拉菜单顶部是否有：
   - ✅ "Copy Name-based Link" 带链接图标
   - ✅ "Copy Permanent Link" 带复制图标
   - ✅ 分隔线
4. 点击任一选项，验证：
   - ✅ 显示成功 Toast 提示
   - ✅ 链接已复制到剪贴板
   - ✅ 菜单自动关闭

### 功能测试
```bash
# 测试 1：复制并访问永久链接
1. 点击 "Copy Permanent Link"
2. 在新标签页粘贴访问
3. 预期：成功打开术语页面，URL 格式为 /glossary/{uuid}

# 测试 2：复制并访问名称链接
1. 点击 "Copy Name-based Link"
2. 在新标签页粘贴访问
3. 预期：成功打开术语页面，URL 格式为 /glossary/{fqn}

# 测试 3：重命名后的链接稳定性
1. 复制术语的永久链接
2. 重命名术语（如 "TestTerm" → "RenamedTerm"）
3. 访问之前复制的永久链接
4. 预期：仍然能正常访问，显示重命名后的内容
```

---

## 问题排查

### 如果图标仍不显示

**检查点 1：** 确认 SVG 文件存在
```bash
ls -la /workspace/openmetadata-ui/src/main/resources/ui/src/assets/svg/link.svg
ls -la /workspace/openmetadata-ui/src/main/resources/ui/src/assets/svg/icon-copy.svg
```

**检查点 2：** 查看浏览器控制台是否有导入错误
```
F12 → Console → 查找 "Failed to load" 或 "Cannot find module"
```

**检查点 3：** 清除构建缓存
```bash
cd /workspace/openmetadata-ui/src/main/resources/ui
rm -rf node_modules/.cache
yarn build
```

### 如果菜单项不显示

**检查点 1：** 验证翻译文件是否正确
```bash
# 英文
grep "copy-fqn-link" /workspace/openmetadata-ui/src/main/resources/ui/src/locale/languages/en-us.json

# 中文
grep "copy-fqn-link" /workspace/openmetadata-ui/src/main/resources/ui/src/locale/languages/zh-cn.json
```

**检查点 2：** 查看浏览器 Network 标签，确认翻译文件已加载

**检查点 3：** 检查 `manageButtonContent` 数组长度
```typescript
// 在浏览器 Console 中
// 应该至少包含 2 个链接选项 + 1 个分隔符 = 长度 >= 3
console.log('manageButtonContent length:', manageButtonContent.length);
```

---

## 已知限制

1. **菜单宽度：** 固定为 350px（`overlayStyle={{ width: '350px' }}`）
   - 如果描述文本过长可能会换行
   
2. **图标大小：** 由 `ManageButtonItemLabel` 组件控制，固定为 18px

3. **版本页面：** 在版本历史页面（`isVersionView = true`）不显示 Manage 按钮

---

## 完成状态

✅ 图标显示问题已修复  
✅ 菜单结构已调整为平铺方式  
✅ 分隔符已添加以区分功能组  
✅ 国际化文本已完整添加（中英文）  
✅ 代码符合项目规范（使用项目自带 SVG 图标）

---

**更新日期：** 2025-10-28  
**状态：** ✅ 已修复，待测试验证
