"""简单格式化器"""

from .base import FormatterBase
from ..core.types import OptimizationResult


class SimpleFormatter(FormatterBase):
    """用于简单请求的格式化器"""
    
    def __init__(self):
        super().__init__("Simple")
    
    def format(self, result: OptimizationResult) -> str:
        """格式化简单请求的结果"""
        output = []
        
        # 优化后的提示
        output.append("**您的优化提示：**")
        output.append(result.optimized_prompt)
        output.append("")
        
        # 主要改变
        output.append("**主要改进：**")
        if result.key_improvements:
            for improvement in result.key_improvements[:2]:  # 只显示前2个
                output.append(f"• {improvement}")
        else:
            output.append("• 提高了清晰度和具体性")
        
        return "\n".join(output)