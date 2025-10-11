# Lyra 使用指南

## 快速开始

### 1. 交互式使用

运行快速开始脚本：

```bash
python3 quickstart.py
```

### 2. 编程方式使用

```python
from src.core import LyraOptimizer, TargetAI, OperatingMode

# 创建优化器
optimizer = LyraOptimizer()

# 基础模式优化
result = optimizer.optimize(
    "写一封邮件",
    TargetAI.CHATGPT,
    OperatingMode.BASIC
)
print(result.optimized_prompt)

# 详细模式优化
result = optimizer.optimize(
    "创建一个完整的项目管理系统",
    TargetAI.CLAUDE,
    OperatingMode.DETAIL
)
print(optimizer.format_response(result))
```

### 3. 自动模式检测

```python
# 让 Lyra 自动决定使用哪种模式
response, needs_input = optimizer.process_user_input(
    "解释什么是机器学习"
)
print(response)
```

## 输入格式

### 完整格式
```
[模式] 使用 [AI平台] — [您的提示]
```

### 示例
- `DETAIL 使用 ChatGPT — 写一篇关于 AI 的文章`
- `BASIC 使用 Claude — 解释递归`
- `使用 Gemini 创建故事`（自动检测模式）
- `设计一个网站`（自动检测模式和平台）

## 操作模式

### BASIC 模式
- 快速优化主要问题
- 立即提供可用的提示
- 适合简单、直接的任务

### DETAIL 模式
- 深度分析和优化
- 提供澄清问题
- 应用多种优化技术
- 适合复杂、多层次的任务

## 支持的 AI 平台

- **ChatGPT**: 对话式交互，Markdown 格式
- **Claude**: 长文本推理，结构化思考
- **Gemini**: 创意任务，多角度分析
- **Other**: 通用最佳实践

## 优化技术

### 基础技术
- 角色分配
- 上下文分层
- 输出规范
- 任务分解

### 高级技术
- 思维链（Chain of Thought）
- 少样本学习（Few-shot Learning）
- 多角度分析
- 约束优化

## 运行示例

### 查看所有演示
```bash
python3 src/examples/demo.py
```

### 查看使用案例
```bash
python3 src/examples/use_cases.py
```

### 运行测试
```bash
python3 test_lyra.py
```

## 常见问题

### Q: 如何选择模式？
A: 简单任务用 BASIC，复杂任务用 DETAIL。不确定时让系统自动检测。

### Q: 哪个 AI 平台最好？
A: 取决于任务类型：
- 对话和通用任务：ChatGPT
- 深度分析和推理：Claude
- 创意和多样化：Gemini

### Q: 可以自定义优化规则吗？
A: 是的，可以通过修改 `src/techniques/` 中的技术实现来自定义。

## 获取帮助

在交互模式中输入 `help` 查看更多帮助信息。