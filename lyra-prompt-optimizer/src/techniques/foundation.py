"""基础优化技术实现"""

from typing import Dict, Any
from .base import TechniqueBase


class RoleAssignmentTechnique(TechniqueBase):
    """角色分配技术"""
    
    def __init__(self):
        super().__init__(
            name="角色分配",
            description="为 AI 分配特定的专业角色以提高响应质量"
        )
    
    def apply(self, prompt: str, context: Dict[str, Any]) -> str:
        """应用角色分配"""
        role = context.get("assigned_role", "你是一位乐于助人的助手")
        return f"{role}。\n\n{prompt}"
    
    def get_example(self) -> str:
        return "你是一位经验丰富的营销专家，擅长创建引人注目的营销文案。"


class ContextLayeringTechnique(TechniqueBase):
    """上下文分层技术"""
    
    def __init__(self):
        super().__init__(
            name="上下文分层",
            description="通过分层提供背景信息来增强理解"
        )
    
    def apply(self, prompt: str, context: Dict[str, Any]) -> str:
        """应用上下文分层"""
        layers = []
        
        # 背景层
        if context.get("background"):
            layers.append(f"背景：{context['background']}")
        
        # 目标层
        if context.get("objective"):
            layers.append(f"目标：{context['objective']}")
        
        # 约束层
        if context.get("constraints"):
            constraints = context['constraints']
            if isinstance(constraints, list):
                constraints = "、".join(constraints)
            layers.append(f"约束条件：{constraints}")
        
        # 组合层次
        if layers:
            context_section = "\n".join(layers)
            return f"{context_section}\n\n任务：{prompt}"
        
        return prompt
    
    def get_example(self) -> str:
        return """背景：我们是一家初创科技公司
目标：提高品牌知名度
约束条件：预算有限、时间紧迫

任务：创建社交媒体营销策略"""


class OutputSpecsTechnique(TechniqueBase):
    """输出规范技术"""
    
    def __init__(self):
        super().__init__(
            name="输出规范",
            description="明确定义期望的输出格式和要求"
        )
    
    def apply(self, prompt: str, context: Dict[str, Any]) -> str:
        """应用输出规范"""
        specs = []
        
        # 格式规范
        if context.get("format"):
            specs.append(f"格式：{context['format']}")
        
        # 长度规范
        if context.get("length"):
            specs.append(f"长度：{context['length']}")
        
        # 风格规范
        if context.get("style"):
            specs.append(f"风格：{context['style']}")
        
        # 其他要求
        if context.get("requirements"):
            reqs = context['requirements']
            if isinstance(reqs, list):
                specs.extend([f"- {req}" for req in reqs])
        
        if specs:
            specs_section = "\n".join(specs)
            return f"{prompt}\n\n输出要求：\n{specs_section}"
        
        return prompt
    
    def get_example(self) -> str:
        return """请写一篇关于人工智能的文章

输出要求：
格式：博客文章
长度：800-1000字
风格：专业但易懂
- 包含至少3个实际应用案例
- 使用副标题组织内容
- 结尾包含行动呼吁"""


class TaskDecompositionTechnique(TechniqueBase):
    """任务分解技术"""
    
    def __init__(self):
        super().__init__(
            name="任务分解",
            description="将复杂任务分解为可管理的子任务"
        )
    
    def apply(self, prompt: str, context: Dict[str, Any]) -> str:
        """应用任务分解"""
        subtasks = context.get("subtasks", [])
        
        if not subtasks and context.get("auto_decompose"):
            # 自动分解逻辑
            subtasks = self._auto_decompose(prompt)
        
        if subtasks:
            task_list = "\n".join([f"{i+1}. {task}" for i, task in enumerate(subtasks)])
            return f"{prompt}\n\n请按以下步骤完成：\n{task_list}"
        
        return prompt
    
    def _auto_decompose(self, prompt: str) -> list:
        """自动分解任务（简化版）"""
        # 这是一个简化的实现，实际应用中可以使用更复杂的逻辑
        keywords = ["并且", "同时", "另外", "然后", "接着"]
        
        for keyword in keywords:
            if keyword in prompt:
                parts = prompt.split(keyword)
                return [part.strip() for part in parts if part.strip()]
        
        return []
    
    def get_example(self) -> str:
        return """创建一个完整的产品发布计划

请按以下步骤完成：
1. 分析目标市场和用户画像
2. 制定产品定位和核心卖点
3. 设计营销策略和渠道
4. 创建时间表和里程碑
5. 准备发布材料和资源"""