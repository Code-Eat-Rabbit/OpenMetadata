"""开发器：4-D 方法论的第三步"""

from typing import List, Dict, Any
from .types import (
    DiagnoseResult, DevelopResult, PromptType, 
    OptimizationTechnique, TargetAI
)


class Developer:
    """负责开发优化策略和增强提示"""
    
    def __init__(self):
        self.technique_mapping = {
            PromptType.CREATIVE: [
                OptimizationTechnique.MULTI_PERSPECTIVE,
                OptimizationTechnique.ROLE_ASSIGNMENT,
                OptimizationTechnique.CONTEXT_LAYERING
            ],
            PromptType.TECHNICAL: [
                OptimizationTechnique.CONSTRAINT_OPTIMIZATION,
                OptimizationTechnique.OUTPUT_SPECS,
                OptimizationTechnique.TASK_DECOMPOSITION
            ],
            PromptType.EDUCATIONAL: [
                OptimizationTechnique.FEW_SHOT_LEARNING,
                OptimizationTechnique.CHAIN_OF_THOUGHT,
                OptimizationTechnique.OUTPUT_SPECS
            ],
            PromptType.COMPLEX: [
                OptimizationTechnique.CHAIN_OF_THOUGHT,
                OptimizationTechnique.TASK_DECOMPOSITION,
                OptimizationTechnique.CONSTRAINT_OPTIMIZATION
            ],
            PromptType.SIMPLE: [
                OptimizationTechnique.ROLE_ASSIGNMENT,
                OptimizationTechnique.CONTEXT_LAYERING,
                OptimizationTechnique.OUTPUT_SPECS
            ]
        }
        
        self.role_templates = {
            PromptType.CREATIVE: "你是一位富有创造力的{domain}专家，擅长生成独特和引人入胜的内容",
            PromptType.TECHNICAL: "你是一位经验丰富的{domain}技术专家，具有深厚的实践经验",
            PromptType.EDUCATIONAL: "你是一位耐心细致的{domain}教育专家，擅长将复杂概念简化",
            PromptType.COMPLEX: "你是一位{domain}领域的高级顾问，精通系统化分析和解决方案设计",
            PromptType.SIMPLE: "你是一位友好的{domain}助手，专注于提供清晰直接的帮助"
        }
        
        self.platform_optimizations = {
            TargetAI.CHATGPT: {
                "section_markers": True,
                "conversation_style": True,
                "token_awareness": True
            },
            TargetAI.CLAUDE: {
                "long_context": True,
                "reasoning_emphasis": True,
                "structured_thinking": True
            },
            TargetAI.GEMINI: {
                "creative_emphasis": True,
                "comparative_analysis": True,
                "multimodal_ready": True
            },
            TargetAI.OTHER: {
                "universal_best_practices": True
            }
        }
    
    def develop(self, prompt: str, diagnose_result: DiagnoseResult, 
                target_ai: TargetAI) -> DevelopResult:
        """开发优化策略"""
        selected_techniques = self._select_techniques(diagnose_result)
        assigned_role = self._assign_role(diagnose_result)
        enhanced_context = self._enhance_context(prompt, diagnose_result)
        logical_structure = self._create_logical_structure(diagnose_result)
        platform_opts = self._get_platform_optimizations(target_ai)
        
        return DevelopResult(
            selected_techniques=selected_techniques,
            assigned_role=assigned_role,
            enhanced_context=enhanced_context,
            logical_structure=logical_structure,
            platform_optimizations=platform_opts
        )
    
    def _select_techniques(self, diagnose_result: DiagnoseResult) -> List[OptimizationTechnique]:
        """选择适合的优化技术"""
        techniques = self.technique_mapping[diagnose_result.prompt_type].copy()
        
        # 根据诊断结果添加额外技术
        if diagnose_result.clarity_gaps:
            techniques.append(OptimizationTechnique.CONTEXT_LAYERING)
        
        if diagnose_result.specificity_issues:
            techniques.append(OptimizationTechnique.OUTPUT_SPECS)
        
        if len(diagnose_result.structure_needs) > 2:
            techniques.append(OptimizationTechnique.TASK_DECOMPOSITION)
        
        # 去重并返回
        return list(dict.fromkeys(techniques))
    
    def _assign_role(self, diagnose_result: DiagnoseResult) -> str:
        """分配 AI 角色"""
        prompt_type = diagnose_result.prompt_type
        role_template = self.role_templates[prompt_type]
        
        # 确定领域
        domain = "全能"  # 默认
        if diagnose_result.structure_needs:
            for need in diagnose_result.structure_needs:
                if "技术" in need:
                    domain = "技术"
                    break
                elif "创意" in need:
                    domain = "创意"
                    break
                elif "教育" in need:
                    domain = "教育"
                    break
        
        return role_template.format(domain=domain)
    
    def _enhance_context(self, prompt: str, diagnose_result: DiagnoseResult) -> str:
        """增强上下文"""
        enhancements = []
        
        # 添加缺失的具体性
        if diagnose_result.specificity_issues:
            enhancements.append("请提供具体、详细的内容")
        
        # 添加清晰度要求
        if diagnose_result.clarity_gaps:
            enhancements.append("确保表达清晰、避免歧义")
        
        # 添加结构要求
        if diagnose_result.structure_needs:
            enhancements.append("使用清晰的结构组织内容")
        
        # 根据完整性分数添加要求
        if diagnose_result.completeness_score < 0.5:
            enhancements.append("请提供完整、全面的回应")
        
        return " | ".join(enhancements) if enhancements else ""
    
    def _create_logical_structure(self, diagnose_result: DiagnoseResult) -> str:
        """创建逻辑结构"""
        structure_parts = []
        
        # 根据提示类型创建基础结构
        if diagnose_result.prompt_type == PromptType.COMPLEX:
            structure_parts.append("1. 问题分析\n2. 解决方案设计\n3. 实施步骤\n4. 预期结果")
        elif diagnose_result.prompt_type == PromptType.EDUCATIONAL:
            structure_parts.append("1. 概念介绍\n2. 详细解释\n3. 实例说明\n4. 总结要点")
        elif diagnose_result.prompt_type == PromptType.TECHNICAL:
            structure_parts.append("1. 技术背景\n2. 实现方案\n3. 代码示例\n4. 注意事项")
        elif diagnose_result.prompt_type == PromptType.CREATIVE:
            structure_parts.append("1. 创意构思\n2. 内容展开\n3. 细节润色\n4. 整体呈现")
        else:
            structure_parts.append("1. 直接回答\n2. 补充说明\n3. 相关建议")
        
        # 根据结构需求调整
        if "需要步骤化的结构" in diagnose_result.structure_needs:
            structure_parts.append("\n使用编号步骤详细说明每个阶段")
        
        if "需要条件逻辑结构" in diagnose_result.structure_needs:
            structure_parts.append("\n明确说明不同条件下的处理方式")
        
        return "\n".join(structure_parts)
    
    def _get_platform_optimizations(self, target_ai: TargetAI) -> Dict[str, Any]:
        """获取平台特定优化"""
        base_opts = self.platform_optimizations.get(target_ai, {})
        
        # 添加平台特定的建议
        if target_ai == TargetAI.CHATGPT:
            base_opts["formatting"] = "使用 Markdown 格式，包括标题、列表和代码块"
            base_opts["interaction"] = "保持对话式的交互风格"
        elif target_ai == TargetAI.CLAUDE:
            base_opts["formatting"] = "使用结构化的章节和清晰的逻辑流程"
            base_opts["reasoning"] = "包含思考过程和推理步骤"
        elif target_ai == TargetAI.GEMINI:
            base_opts["formatting"] = "使用视觉友好的格式，适合创意表达"
            base_opts["analysis"] = "提供多角度的比较和分析"
        
        return base_opts