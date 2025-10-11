"""复杂格式化器"""

from .base import FormatterBase
from ..core.types import OptimizationResult


class ComplexFormatter(FormatterBase):
    """用于复杂请求的格式化器"""
    
    def __init__(self):
        super().__init__("Complex")
    
    def format(self, result: OptimizationResult) -> str:
        """格式化复杂请求的结果"""
        output = []
        
        # 优化后的提示
        output.append("**您的优化提示：**")
        output.append("```")
        output.append(result.optimized_prompt)
        output.append("```")
        output.append("")
        
        # 关键改进
        output.append("**关键改进：**")
        if result.key_improvements:
            for improvement in result.key_improvements:
                output.append(f"• {improvement}")
        output.append("")
        
        # 应用的技术
        if result.techniques_applied:
            output.append("**应用的技术：**")
            techniques_str = "、".join(result.techniques_applied)
            output.append(techniques_str)
            output.append("")
        
        # 专业提示
        if result.pro_tip:
            output.append("**专业提示：**")
            output.append(result.pro_tip)
            output.append("")
        
        # 澄清问题（如果有）
        if result.clarifying_questions:
            output.append("**为了进一步优化，请回答以下问题：**")
            for i, question in enumerate(result.clarifying_questions, 1):
                output.append(f"{i}. {question}")
        
        return "\n".join(output)