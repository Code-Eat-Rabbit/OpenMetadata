"""详细模式实现"""

from typing import List, Optional
from .base import ModeBase
from ..core.types import OptimizationResult, TargetAI
from ..core.deconstructor import Deconstructor
from ..core.diagnoser import Diagnoser
from ..core.developer import Developer
from ..techniques import (
    RoleAssignmentTechnique,
    ContextLayeringTechnique,
    OutputSpecsTechnique,
    TaskDecompositionTechnique,
    ChainOfThoughtTechnique,
    FewShotLearningTechnique,
    MultiPerspectiveTechnique,
    ConstraintOptimizationTechnique
)


class DetailMode(ModeBase):
    """详细模式：收集上下文，提出澄清问题，提供全面优化"""
    
    def __init__(self):
        super().__init__(
            name="DETAIL",
            description="详细优化模式，包含智能默认值和针对性的澄清问题"
        )
        self.deconstructor = Deconstructor()
        self.diagnoser = Diagnoser()
        self.developer = Developer()
        
        # 初始化技术库
        self.techniques = {
            "role_assignment": RoleAssignmentTechnique(),
            "context_layering": ContextLayeringTechnique(),
            "output_specs": OutputSpecsTechnique(),
            "task_decomposition": TaskDecompositionTechnique(),
            "chain_of_thought": ChainOfThoughtTechnique(),
            "few_shot_learning": FewShotLearningTechnique(),
            "multi_perspective": MultiPerspectiveTechnique(),
            "constraint_optimization": ConstraintOptimizationTechnique()
        }
    
    def process(self, prompt: str, target_ai: TargetAI) -> OptimizationResult:
        """处理详细模式的优化"""
        # 4-D 方法论流程
        deconstruct_result = self.deconstructor.deconstruct(prompt)
        diagnose_result = self.diagnoser.diagnose(prompt, deconstruct_result)
        develop_result = self.developer.develop(prompt, diagnose_result, target_ai)
        
        # 生成澄清问题
        clarifying_questions = self._generate_clarifying_questions(
            deconstruct_result, diagnose_result
        )
        
        # 构建优化后的提示
        optimized_prompt = self._build_optimized_prompt(
            prompt, deconstruct_result, diagnose_result, develop_result
        )
        
        # 收集关键改进
        key_improvements = self._collect_key_improvements(
            deconstruct_result, diagnose_result, develop_result
        )
        
        # 收集应用的技术
        techniques_applied = [
            tech.value for tech in develop_result.selected_techniques
        ]
        
        # 生成专业提示
        pro_tip = self._generate_pro_tip(diagnose_result, target_ai)
        
        return OptimizationResult(
            original_prompt=prompt,
            optimized_prompt=optimized_prompt,
            key_improvements=key_improvements,
            techniques_applied=techniques_applied,
            pro_tip=pro_tip,
            clarifying_questions=clarifying_questions,
            complexity_level="详细"
        )
    
    def get_clarifying_questions(self, prompt: str) -> Optional[List[str]]:
        """获取澄清问题"""
        deconstruct_result = self.deconstructor.deconstruct(prompt)
        diagnose_result = self.diagnoser.diagnose(prompt, deconstruct_result)
        
        return self._generate_clarifying_questions(deconstruct_result, diagnose_result)
    
    def _generate_clarifying_questions(self, deconstruct_result, diagnose_result) -> List[str]:
        """生成 2-3 个针对性的澄清问题"""
        questions = []
        
        # 基于缺失元素生成问题
        if "目标受众" in deconstruct_result.missing_elements:
            questions.append("这个内容的目标受众是谁？（例如：专业人士、初学者、普通用户）")
        
        if "具体目的" in deconstruct_result.missing_elements:
            questions.append("这个内容的主要目的是什么？（例如：教育、说服、娱乐、信息传递）")
        
        if "输出格式规范" in deconstruct_result.missing_elements:
            questions.append("您希望的输出格式是什么？（例如：列表、段落、表格、代码）")
        
        # 基于诊断结果生成问题
        if diagnose_result.specificity_issues:
            if "缺少quantity的具体说明" in diagnose_result.specificity_issues:
                questions.append("需要多少内容？（例如：字数、项目数、示例数量）")
        
        # 限制为最多 3 个问题
        return questions[:3]
    
    def _build_optimized_prompt(self, original_prompt, deconstruct_result, 
                                diagnose_result, develop_result) -> str:
        """构建优化后的提示"""
        parts = []
        
        # 1. 角色分配
        if develop_result.assigned_role:
            parts.append(develop_result.assigned_role)
        
        # 2. 上下文增强
        if develop_result.enhanced_context:
            parts.append(f"上下文：{develop_result.enhanced_context}")
        
        # 3. 核心任务
        parts.append(f"任务：{original_prompt}")
        
        # 4. 应用选定的技术
        technique_context = {
            "assigned_role": develop_result.assigned_role,
            "constraints": deconstruct_result.constraints,
            "requirements": deconstruct_result.output_requirements,
            "format": "结构化",
            "style": "专业清晰"
        }
        
        # 应用输出规范技术
        if "OUTPUT_SPECS" in [t.name for t in develop_result.selected_techniques]:
            output_specs = self.techniques["output_specs"].apply("", technique_context)
            if output_specs:
                parts.append(output_specs)
        
        # 5. 逻辑结构
        if develop_result.logical_structure:
            parts.append(f"请按以下结构组织：\n{develop_result.logical_structure}")
        
        # 6. 平台特定优化
        platform_notes = self._get_platform_notes(develop_result.platform_optimizations)
        if platform_notes:
            parts.append(platform_notes)
        
        return "\n\n".join(parts)
    
    def _collect_key_improvements(self, deconstruct_result, diagnose_result, 
                                  develop_result) -> List[str]:
        """收集关键改进"""
        improvements = []
        
        # 基于诊断结果的改进
        if diagnose_result.clarity_gaps:
            improvements.append("消除了模糊表达，提高了清晰度")
        
        if diagnose_result.specificity_issues:
            improvements.append("添加了具体的要求和规范")
        
        if develop_result.assigned_role:
            improvements.append("分配了专业角色以提高响应质量")
        
        if develop_result.logical_structure:
            improvements.append("提供了清晰的逻辑结构")
        
        # 基于技术的改进
        tech_names = [t.name for t in develop_result.selected_techniques]
        if "CHAIN_OF_THOUGHT" in tech_names:
            improvements.append("添加了思维链引导以提高推理质量")
        
        if "FEW_SHOT_LEARNING" in tech_names:
            improvements.append("提供了示例以确保输出格式正确")
        
        return improvements[:4]  # 限制为最多 4 个改进
    
    def _generate_pro_tip(self, diagnose_result, target_ai: TargetAI) -> str:
        """生成专业提示"""
        if target_ai == TargetAI.CHATGPT:
            return "在 ChatGPT 中，您可以通过后续对话进一步细化结果"
        elif target_ai == TargetAI.CLAUDE:
            return "Claude 擅长长文本和深度推理，可以要求更详细的分析"
        elif target_ai == TargetAI.GEMINI:
            return "Gemini 在创意任务中表现出色，可以要求多个创意变体"
        else:
            return "记得根据初始响应的质量进行迭代优化"
    
    def _get_platform_notes(self, platform_optimizations) -> str:
        """获取平台特定的注释"""
        notes = []
        
        if platform_optimizations.get("formatting"):
            notes.append(platform_optimizations["formatting"])
        
        if platform_optimizations.get("interaction"):
            notes.append(platform_optimizations["interaction"])
        
        if platform_optimizations.get("reasoning"):
            notes.append(platform_optimizations["reasoning"])
        
        if notes:
            return "附加说明：" + " | ".join(notes)
        
        return ""