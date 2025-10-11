"""基础模式实现"""

from typing import List, Optional
from .base import ModeBase
from ..core.types import OptimizationResult, TargetAI
from ..core.deconstructor import Deconstructor
from ..core.diagnoser import Diagnoser
from ..techniques import RoleAssignmentTechnique, OutputSpecsTechnique


class BasicMode(ModeBase):
    """基础模式：快速修复主要问题，立即可用"""
    
    def __init__(self):
        super().__init__(
            name="BASIC",
            description="快速优化模式，修复主要问题并提供即用提示"
        )
        self.deconstructor = Deconstructor()
        self.diagnoser = Diagnoser()
        
        # 基础模式只使用核心技术
        self.role_technique = RoleAssignmentTechnique()
        self.output_technique = OutputSpecsTechnique()
    
    def process(self, prompt: str, target_ai: TargetAI) -> OptimizationResult:
        """处理基础模式的优化"""
        # 快速分析
        deconstruct_result = self.deconstructor.deconstruct(prompt)
        diagnose_result = self.diagnoser.diagnose(prompt, deconstruct_result)
        
        # 构建优化提示
        optimized_prompt = self._quick_optimize(
            prompt, deconstruct_result, diagnose_result, target_ai
        )
        
        # 识别主要改进
        key_improvements = self._identify_quick_improvements(
            deconstruct_result, diagnose_result
        )
        
        return OptimizationResult(
            original_prompt=prompt,
            optimized_prompt=optimized_prompt,
            key_improvements=key_improvements,
            techniques_applied=["角色分配", "输出规范"],
            pro_tip=None,
            clarifying_questions=None,
            complexity_level="基础"
        )
    
    def get_clarifying_questions(self, prompt: str) -> Optional[List[str]]:
        """基础模式不提供澄清问题"""
        return None
    
    def _quick_optimize(self, prompt, deconstruct_result, diagnose_result, 
                        target_ai: TargetAI) -> str:
        """快速优化提示"""
        parts = []
        
        # 1. 分配基本角色
        role = self._assign_quick_role(diagnose_result.prompt_type)
        parts.append(role)
        
        # 2. 澄清核心任务
        clarified_task = self._clarify_task(prompt, deconstruct_result)
        parts.append(f"任务：{clarified_task}")
        
        # 3. 添加基本输出要求
        basic_requirements = self._get_basic_requirements(diagnose_result)
        if basic_requirements:
            parts.append(f"要求：\n{basic_requirements}")
        
        # 4. 平台快速优化
        platform_tip = self._get_platform_quick_tip(target_ai)
        if platform_tip:
            parts.append(platform_tip)
        
        return "\n\n".join(parts)
    
    def _assign_quick_role(self, prompt_type) -> str:
        """快速分配角色"""
        role_map = {
            "CREATIVE": "你是一位富有创造力的内容创作专家",
            "TECHNICAL": "你是一位经验丰富的技术专家",
            "EDUCATIONAL": "你是一位耐心的教育工作者",
            "COMPLEX": "你是一位善于解决复杂问题的顾问",
            "SIMPLE": "你是一位友好的助手"
        }
        
        return role_map.get(prompt_type.name, "你是一位专业的助手")
    
    def _clarify_task(self, prompt: str, deconstruct_result) -> str:
        """澄清任务描述"""
        # 如果原始提示太短或太模糊，添加一些上下文
        if len(prompt) < 20 or not deconstruct_result.key_entities:
            return f"{prompt}（请提供详细、具体的回应）"
        
        return prompt
    
    def _get_basic_requirements(self, diagnose_result) -> str:
        """获取基本要求"""
        requirements = []
        
        # 基于诊断结果添加基本要求
        if diagnose_result.clarity_gaps:
            requirements.append("- 使用清晰、具体的语言")
        
        if diagnose_result.specificity_issues:
            requirements.append("- 提供具体的细节和示例")
        
        if not requirements:
            # 默认要求
            requirements = [
                "- 结构清晰",
                "- 内容完整",
                "- 易于理解"
            ]
        
        return "\n".join(requirements)
    
    def _get_platform_quick_tip(self, target_ai: TargetAI) -> str:
        """获取平台快速提示"""
        tips = {
            TargetAI.CHATGPT: "请使用 Markdown 格式组织内容。",
            TargetAI.CLAUDE: "请展示你的推理过程。",
            TargetAI.GEMINI: "请提供创造性的解决方案。",
            TargetAI.OTHER: "请提供结构化的响应。"
        }
        
        return tips.get(target_ai, "")
    
    def _identify_quick_improvements(self, deconstruct_result, 
                                     diagnose_result) -> List[str]:
        """识别快速改进"""
        improvements = []
        
        if diagnose_result.clarity_gaps:
            improvements.append("提高了表达清晰度")
        
        if not deconstruct_result.output_requirements:
            improvements.append("添加了基本输出要求")
        
        improvements.append("分配了适合的 AI 角色")
        
        if diagnose_result.completeness_score < 0.5:
            improvements.append("补充了缺失的关键信息")
        
        return improvements[:3]  # 最多返回 3 个改进