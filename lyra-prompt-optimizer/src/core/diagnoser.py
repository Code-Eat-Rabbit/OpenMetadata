"""诊断器：4-D 方法论的第二步"""

from typing import List
from .types import DeconstructResult, DiagnoseResult, PromptType


class Diagnoser:
    """负责诊断提示中的问题和改进机会"""
    
    def __init__(self):
        self.clarity_indicators = {
            "vague_words": ["thing", "stuff", "something", "somehow", "maybe", "probably"],
            "unclear_pronouns": ["it", "this", "that", "they", "them"],
            "ambiguous_terms": ["good", "bad", "nice", "okay", "fine", "better"]
        }
        
        self.specificity_checkers = {
            "quantity": ["how many", "how much", "number", "amount"],
            "quality": ["what kind", "which type", "what format"],
            "timing": ["when", "how long", "deadline", "timeframe"],
            "scope": ["what aspects", "which parts", "boundaries"]
        }
    
    def diagnose(self, prompt: str, deconstruct_result: DeconstructResult) -> DiagnoseResult:
        """诊断提示的问题"""
        clarity_gaps = self._identify_clarity_gaps(prompt, deconstruct_result)
        ambiguities = self._identify_ambiguities(prompt)
        specificity_issues = self._check_specificity(prompt, deconstruct_result)
        completeness_score = self._calculate_completeness(deconstruct_result)
        structure_needs = self._analyze_structure_needs(prompt, deconstruct_result)
        prompt_type = self._classify_prompt_type(prompt, deconstruct_result)
        
        return DiagnoseResult(
            clarity_gaps=clarity_gaps,
            ambiguities=ambiguities,
            specificity_issues=specificity_issues,
            completeness_score=completeness_score,
            structure_needs=structure_needs,
            prompt_type=prompt_type
        )
    
    def _identify_clarity_gaps(self, prompt: str, deconstruct_result: DeconstructResult) -> List[str]:
        """识别清晰度差距"""
        gaps = []
        prompt_lower = prompt.lower()
        
        # 检查模糊词汇
        for vague_word in self.clarity_indicators["vague_words"]:
            if vague_word in prompt_lower:
                gaps.append(f"使用了模糊词汇 '{vague_word}'")
        
        # 检查不清晰的代词
        words = prompt_lower.split()
        for i, word in enumerate(words):
            if word in self.clarity_indicators["unclear_pronouns"]:
                # 检查代词是否有明确的先行词
                if i == 0 or not self._has_clear_antecedent(words, i):
                    gaps.append(f"代词 '{word}' 缺乏明确指代")
        
        # 检查核心意图的清晰度
        if len(deconstruct_result.core_intent) < 10:
            gaps.append("核心意图过于简短，需要更多细节")
        
        return gaps
    
    def _identify_ambiguities(self, prompt: str) -> List[str]:
        """识别歧义"""
        ambiguities = []
        prompt_lower = prompt.lower()
        
        # 检查歧义术语
        for term in self.clarity_indicators["ambiguous_terms"]:
            if term in prompt_lower:
                ambiguities.append(f"术语 '{term}' 需要更具体的定义")
        
        # 检查多重解释的可能性
        if "or" in prompt_lower:
            ambiguities.append("包含 'or' 选择，可能导致多种解释")
        
        # 检查范围歧义
        if any(word in prompt_lower for word in ["some", "few", "several", "many"]):
            ambiguities.append("数量描述不精确")
        
        return ambiguities
    
    def _check_specificity(self, prompt: str, deconstruct_result: DeconstructResult) -> List[str]:
        """检查具体性"""
        issues = []
        prompt_lower = prompt.lower()
        
        # 检查各个维度的具体性
        for dimension, keywords in self.specificity_checkers.items():
            has_specificity = any(keyword in prompt_lower for keyword in keywords)
            if not has_specificity and dimension in ["quantity", "quality"]:
                issues.append(f"缺少{dimension}的具体说明")
        
        # 检查输出格式的具体性
        if not deconstruct_result.output_requirements:
            issues.append("缺少明确的输出格式要求")
        
        # 检查示例的具体性
        if "example" not in prompt_lower and "e.g." not in prompt_lower:
            issues.append("缺少具体示例作为参考")
        
        return issues
    
    def _calculate_completeness(self, deconstruct_result: DeconstructResult) -> float:
        """计算完整性分数"""
        score = 0.0
        total_checks = 7
        
        # 检查各个组成部分
        if deconstruct_result.core_intent:
            score += 1
        if deconstruct_result.key_entities:
            score += 1
        if deconstruct_result.output_requirements:
            score += 1
        if deconstruct_result.constraints:
            score += 1
        if deconstruct_result.context.get("has_examples"):
            score += 1
        if deconstruct_result.context.get("tone_indicators"):
            score += 1
        if not deconstruct_result.missing_elements:
            score += 1
        
        return score / total_checks
    
    def _analyze_structure_needs(self, prompt: str, deconstruct_result: DeconstructResult) -> List[str]:
        """分析结构需求"""
        needs = []
        
        # 根据提示长度确定结构需求
        if len(prompt) > 200:
            needs.append("需要分段或使用列表结构")
        
        # 根据复杂度确定结构需求
        if len(deconstruct_result.output_requirements) > 3:
            needs.append("需要明确的任务分解")
        
        # 检查是否需要步骤化
        if any(word in prompt.lower() for word in ["process", "steps", "procedure", "workflow"]):
            needs.append("需要步骤化的结构")
        
        # 检查是否需要条件逻辑
        if any(word in prompt.lower() for word in ["if", "when", "depending", "based on"]):
            needs.append("需要条件逻辑结构")
        
        return needs
    
    def _classify_prompt_type(self, prompt: str, deconstruct_result: DeconstructResult) -> PromptType:
        """分类提示类型"""
        prompt_lower = prompt.lower()
        domain_indicators = deconstruct_result.context.get("domain_indicators", [])
        
        # 创意型
        if any(word in prompt_lower for word in ["creative", "story", "poem", "design", "imagine"]):
            return PromptType.CREATIVE
        
        # 技术型
        if any(word in prompt_lower for word in ["code", "technical", "api", "function", "algorithm"]):
            return PromptType.TECHNICAL
        
        # 教育型
        if any(word in prompt_lower for word in ["explain", "teach", "learn", "understand", "tutorial"]):
            return PromptType.EDUCATIONAL
        
        # 复杂型（基于多个要求或约束）
        if (len(deconstruct_result.output_requirements) > 3 or 
            len(deconstruct_result.constraints) > 2 or
            len(prompt) > 300):
            return PromptType.COMPLEX
        
        # 默认为简单型
        return PromptType.SIMPLE
    
    def _has_clear_antecedent(self, words: List[str], pronoun_index: int) -> bool:
        """检查代词是否有明确的先行词"""
        # 简单的启发式：检查前面3个词是否包含名词
        start = max(0, pronoun_index - 3)
        preceding_words = words[start:pronoun_index]
        
        # 检查是否有可能的名词（简化版本）
        for word in preceding_words:
            if len(word) > 3 and not word in self.clarity_indicators["unclear_pronouns"]:
                return True
        
        return False