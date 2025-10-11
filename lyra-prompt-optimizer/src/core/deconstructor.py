"""解构器：4-D 方法论的第一步"""

import re
from typing import List, Dict, Any
from .types import DeconstructResult


class Deconstructor:
    """负责解构用户输入，提取关键信息"""
    
    def __init__(self):
        self.entity_patterns = [
            r'\b[A-Z][a-z]+(?:\s+[A-Z][a-z]+)*\b',  # 专有名词
            r'"[^"]*"',  # 引号内容
            r'\b\d+\b',  # 数字
            r'\b(?:email|report|article|code|script|function|API|database)\b',  # 常见实体
        ]
        
        self.requirement_keywords = [
            'create', 'write', 'generate', 'make', 'build', 'analyze',
            'explain', 'summarize', 'compare', 'design', 'implement'
        ]
        
        self.constraint_keywords = [
            'must', 'should', 'need', 'require', 'limit', 'within',
            'no more than', 'at least', 'exactly', 'avoid'
        ]
    
    def deconstruct(self, prompt: str) -> DeconstructResult:
        """解构用户提示"""
        return DeconstructResult(
            core_intent=self._extract_core_intent(prompt),
            key_entities=self._extract_entities(prompt),
            context=self._extract_context(prompt),
            output_requirements=self._extract_requirements(prompt),
            constraints=self._extract_constraints(prompt),
            missing_elements=self._identify_missing_elements(prompt)
        )
    
    def _extract_core_intent(self, prompt: str) -> str:
        """提取核心意图"""
        prompt_lower = prompt.lower()
        
        for keyword in self.requirement_keywords:
            if keyword in prompt_lower:
                # 找到动词后的内容作为核心意图
                pattern = rf'\b{keyword}\s+(.+?)(?:\.|,|;|$)'
                match = re.search(pattern, prompt_lower, re.IGNORECASE)
                if match:
                    return match.group(1).strip()
        
        # 如果没有找到特定关键词，返回整个提示的简化版本
        return prompt.strip()[:100] + "..." if len(prompt) > 100 else prompt.strip()
    
    def _extract_entities(self, prompt: str) -> List[str]:
        """提取关键实体"""
        entities = []
        
        for pattern in self.entity_patterns:
            matches = re.findall(pattern, prompt)
            entities.extend(matches)
        
        # 去重并保持顺序
        seen = set()
        unique_entities = []
        for entity in entities:
            if entity not in seen:
                seen.add(entity)
                unique_entities.append(entity)
        
        return unique_entities
    
    def _extract_context(self, prompt: str) -> Dict[str, Any]:
        """提取上下文信息"""
        context = {
            "length": len(prompt),
            "has_examples": "example" in prompt.lower() or "e.g." in prompt.lower(),
            "has_specific_format": any(fmt in prompt.lower() for fmt in ["format", "structure", "template"]),
            "tone_indicators": self._extract_tone_indicators(prompt),
            "domain_indicators": self._extract_domain_indicators(prompt)
        }
        
        return context
    
    def _extract_tone_indicators(self, prompt: str) -> List[str]:
        """提取语气指示器"""
        tone_keywords = [
            "formal", "informal", "professional", "casual", "friendly",
            "technical", "simple", "detailed", "concise", "comprehensive"
        ]
        
        found_tones = []
        prompt_lower = prompt.lower()
        
        for tone in tone_keywords:
            if tone in prompt_lower:
                found_tones.append(tone)
        
        return found_tones
    
    def _extract_domain_indicators(self, prompt: str) -> List[str]:
        """提取领域指示器"""
        domain_keywords = [
            "marketing", "technical", "educational", "business", "scientific",
            "creative", "legal", "medical", "financial", "academic"
        ]
        
        found_domains = []
        prompt_lower = prompt.lower()
        
        for domain in domain_keywords:
            if domain in prompt_lower:
                found_domains.append(domain)
        
        return found_domains
    
    def _extract_requirements(self, prompt: str) -> List[str]:
        """提取输出要求"""
        requirements = []
        
        # 查找明确的要求陈述
        requirement_patterns = [
            r'(?:must|should|need to|has to)\s+([^.,;]+)',
            r'(?:include|contain|have)\s+([^.,;]+)',
            r'(?:\d+)\s+(?:words|sentences|paragraphs|pages)',
            r'(?:in|using)\s+(?:Python|JavaScript|Java|C\+\+|format|style)'
        ]
        
        for pattern in requirement_patterns:
            matches = re.findall(pattern, prompt, re.IGNORECASE)
            requirements.extend(matches)
        
        return requirements
    
    def _extract_constraints(self, prompt: str) -> List[str]:
        """提取约束条件"""
        constraints = []
        
        # 查找约束相关的陈述
        constraint_patterns = [
            r'(?:no more than|at most|maximum)\s+([^.,;]+)',
            r'(?:at least|minimum)\s+([^.,;]+)',
            r'(?:avoid|don\'t|do not|without)\s+([^.,;]+)',
            r'(?:limit|restrict)\s+([^.,;]+)'
        ]
        
        for pattern in constraint_patterns:
            matches = re.findall(pattern, prompt, re.IGNORECASE)
            constraints.extend(matches)
        
        return constraints
    
    def _identify_missing_elements(self, prompt: str) -> List[str]:
        """识别缺失的元素"""
        missing = []
        prompt_lower = prompt.lower()
        
        # 检查常见的缺失元素
        if not any(word in prompt_lower for word in ["format", "structure", "style"]):
            missing.append("输出格式规范")
        
        if not any(word in prompt_lower for word in ["audience", "reader", "user", "for"]):
            missing.append("目标受众")
        
        if not any(word in prompt_lower for word in ["purpose", "goal", "objective", "aim"]):
            missing.append("具体目的")
        
        if not re.search(r'\d+', prompt):
            missing.append("具体数量或长度要求")
        
        if not any(word in prompt_lower for word in ["example", "like", "such as", "similar"]):
            missing.append("参考示例")
        
        return missing