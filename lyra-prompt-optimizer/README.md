# Lyra AI 提示优化专家

Lyra 是一个高级 AI 提示优化系统，旨在将模糊的用户请求转换为精确、有效的 AI 提示，从而在所有平台上释放 AI 的全部潜力。

## 核心特性

- **4-D 方法论**：解构、诊断、开发、交付
- **多平台支持**：ChatGPT、Claude、Gemini 等
- **双模式操作**：DETAIL（详细）和 BASIC（基础）模式
- **智能优化技术**：从基础到高级的多种技术
- **结构化响应格式**：根据复杂度自适应

## 快速开始

```python
from lyra_optimizer import LyraOptimizer

# 初始化优化器
optimizer = LyraOptimizer()

# 优化提示
result = optimizer.optimize(
    prompt="写一封营销邮件",
    target_ai="ChatGPT",
    mode="DETAIL"
)

print(result.optimized_prompt)
```

## 项目结构

```
lyra-prompt-optimizer/
├── src/
│   ├── core/           # 核心 4-D 方法论实现
│   ├── techniques/     # 优化技术库
│   ├── modes/          # DETAIL 和 BASIC 模式实现
│   ├── formatters/     # 响应格式化器
│   └── examples/       # 示例和测试用例
├── tests/              # 单元测试
└── docs/               # 详细文档
```

## 4-D 方法论

### 1. 解构（Deconstruct）
- 提取核心意图、关键实体和上下文
- 识别输出要求和约束
- 映射已提供内容与缺失内容

### 2. 诊断（Diagnose）
- 审查清晰度差距和歧义
- 检查具体性和完整性
- 评估结构和复杂性需求

### 3. 开发（Develop）
- 根据请求类型选择最佳技术
- 分配适当的 AI 角色/专业知识
- 增强上下文并实现逻辑结构

### 4. 交付（Deliver）
- 构建优化后的提示
- 根据复杂度格式化
- 提供实施指导

## 许可证

MIT License