"""高级优化技术实现"""

from typing import Dict, Any
from .base import TechniqueBase


class ChainOfThoughtTechnique(TechniqueBase):
    """思维链技术"""
    
    def __init__(self):
        super().__init__(
            name="思维链",
            description="引导 AI 展示推理过程，提高复杂问题的解决质量"
        )
    
    def apply(self, prompt: str, context: Dict[str, Any]) -> str:
        """应用思维链"""
        cot_prompt = f"{prompt}\n\n请按以下方式回答：\n"
        cot_prompt += "1. 首先，让我理解问题的关键要素\n"
        cot_prompt += "2. 接下来，我将分析可能的解决方案\n"
        cot_prompt += "3. 然后，我会评估每个方案的优缺点\n"
        cot_prompt += "4. 最后，我将提供最佳建议并解释原因\n"
        cot_prompt += "\n请展示你的思考过程。"
        
        return cot_prompt
    
    def get_example(self) -> str:
        return """如何提高团队的远程协作效率？

请按以下方式回答：
1. 首先，让我理解问题的关键要素
2. 接下来，我将分析可能的解决方案
3. 然后，我会评估每个方案的优缺点
4. 最后，我将提供最佳建议并解释原因

请展示你的思考过程。"""


class FewShotLearningTechnique(TechniqueBase):
    """少样本学习技术"""
    
    def __init__(self):
        super().__init__(
            name="少样本学习",
            description="通过提供示例来引导 AI 生成期望格式的输出"
        )
    
    def apply(self, prompt: str, context: Dict[str, Any]) -> str:
        """应用少样本学习"""
        examples = context.get("examples", [])
        
        if not examples:
            # 提供通用示例框架
            examples = [
                {"input": "示例输入1", "output": "示例输出1"},
                {"input": "示例输入2", "output": "示例输出2"}
            ]
        
        example_text = "以下是一些示例：\n\n"
        for i, example in enumerate(examples, 1):
            example_text += f"示例 {i}：\n"
            example_text += f"输入：{example.get('input', '...')}\n"
            example_text += f"输出：{example.get('output', '...')}\n\n"
        
        return f"{example_text}现在，请处理以下内容：\n{prompt}"
    
    def get_example(self) -> str:
        return """以下是一些示例：

示例 1：
输入：将"机器学习很有趣"翻译成英文
输出：Machine learning is interesting

示例 2：
输入：将"人工智能改变世界"翻译成英文
输出：Artificial intelligence changes the world

现在，请处理以下内容：
将"深度学习推动创新"翻译成英文"""


class MultiPerspectiveTechnique(TechniqueBase):
    """多角度分析技术"""
    
    def __init__(self):
        super().__init__(
            name="多角度分析",
            description="从多个视角分析问题，提供全面的见解"
        )
    
    def apply(self, prompt: str, context: Dict[str, Any]) -> str:
        """应用多角度分析"""
        perspectives = context.get("perspectives", [
            "用户角度",
            "技术角度",
            "商业角度",
            "社会影响角度"
        ])
        
        perspective_prompt = f"{prompt}\n\n请从以下角度进行分析：\n"
        for perspective in perspectives:
            perspective_prompt += f"- {perspective}\n"
        
        perspective_prompt += "\n为每个角度提供独特的见解和建议。"
        
        return perspective_prompt
    
    def get_example(self) -> str:
        return """评估在公司中实施 AI 聊天机器人的可行性

请从以下角度进行分析：
- 用户角度
- 技术角度
- 商业角度
- 社会影响角度

为每个角度提供独特的见解和建议。"""


class ConstraintOptimizationTechnique(TechniqueBase):
    """约束优化技术"""
    
    def __init__(self):
        super().__init__(
            name="约束优化",
            description="明确定义和优化各种约束条件"
        )
    
    def apply(self, prompt: str, context: Dict[str, Any]) -> str:
        """应用约束优化"""
        constraints = context.get("constraints", {})
        
        constraint_text = ""
        
        # 硬约束（必须满足）
        hard_constraints = constraints.get("hard", [])
        if hard_constraints:
            constraint_text += "必须满足的条件：\n"
            for constraint in hard_constraints:
                constraint_text += f"✓ {constraint}\n"
            constraint_text += "\n"
        
        # 软约束（尽量满足）
        soft_constraints = constraints.get("soft", [])
        if soft_constraints:
            constraint_text += "优先考虑的条件：\n"
            for constraint in soft_constraints:
                constraint_text += f"• {constraint}\n"
            constraint_text += "\n"
        
        # 优化目标
        optimization_goals = constraints.get("optimize_for", [])
        if optimization_goals:
            constraint_text += "优化目标：\n"
            for goal in optimization_goals:
                constraint_text += f"→ {goal}\n"
        
        if constraint_text:
            return f"{prompt}\n\n{constraint_text}"
        
        return prompt
    
    def get_example(self) -> str:
        return """设计一个移动应用的用户界面

必须满足的条件：
✓ 符合 iOS 和 Android 设计规范
✓ 支持无障碍访问
✓ 加载时间不超过 3 秒

优先考虑的条件：
• 直观的导航结构
• 现代简洁的视觉风格
• 支持暗色模式

优化目标：
→ 最大化用户参与度
→ 最小化学习曲线
→ 提高任务完成效率"""