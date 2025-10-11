"""Lyra 优化器主类"""

import re
from typing import Optional, Tuple
from .types import TargetAI, OperatingMode, OptimizationResult
from ..modes import DetailMode, BasicMode
from ..formatters import SimpleFormatter, ComplexFormatter


class LyraOptimizer:
    """Lyra AI 提示优化专家系统"""
    
    WELCOME_MESSAGE = """你好！我是 Lyra，您的 AI 提示优化专家。我将模糊的请求转换为精确、有效的提示，从而获得更好的结果。

**我需要了解的信息：**
- **目标 AI：** ChatGPT、Claude、Gemini 或 Other
- **提示风格：** DETAIL（我会先提出澄清问题）或 BASIC（快速优化）

**示例：**
- "DETAIL 使用 ChatGPT — 写一封营销邮件"
- "BASIC 使用 Claude — 帮助我的简历"

只需分享您的初步提示，我会处理优化！"""
    
    def __init__(self):
        self.detail_mode = DetailMode()
        self.basic_mode = BasicMode()
        self.simple_formatter = SimpleFormatter()
        self.complex_formatter = ComplexFormatter()
    
    def get_welcome_message(self) -> str:
        """获取欢迎消息"""
        return self.WELCOME_MESSAGE
    
    def optimize(self, prompt: str, target_ai: Optional[TargetAI] = None,
                 mode: Optional[OperatingMode] = None) -> OptimizationResult:
        """优化提示"""
        # 如果未指定，尝试从提示中提取参数
        if not target_ai or not mode:
            extracted_ai, extracted_mode = self._extract_parameters(prompt)
            target_ai = target_ai or extracted_ai or TargetAI.OTHER
            mode = mode or extracted_mode or self._auto_detect_mode(prompt)
        
        # 根据模式处理
        if mode == OperatingMode.DETAIL:
            result = self.detail_mode.process(prompt, target_ai)
        else:
            result = self.basic_mode.process(prompt, target_ai)
        
        return result
    
    def format_response(self, result: OptimizationResult) -> str:
        """格式化响应"""
        # 根据复杂度选择格式化器
        if result.complexity_level == "基础" or len(result.original_prompt) < 50:
            return self.simple_formatter.format(result)
        else:
            return self.complex_formatter.format(result)
    
    def process_user_input(self, user_input: str) -> Tuple[str, bool]:
        """处理用户输入，返回格式化的响应和是否需要进一步输入"""
        # 提取参数
        target_ai, mode = self._extract_parameters(user_input)
        
        # 清理提示（去除参数部分）
        clean_prompt = self._clean_prompt(user_input)
        
        if not clean_prompt:
            return "请提供您想要优化的提示内容。", True
        
        # 如果未指定参数，使用自动检测
        if not target_ai:
            target_ai = TargetAI.OTHER
        if not mode:
            mode = self._auto_detect_mode(clean_prompt)
            # 通知用户自动检测的结果
            mode_name = "DETAIL" if mode == OperatingMode.DETAIL else "BASIC"
            auto_detect_msg = f"（自动检测到复杂度，使用 {mode_name} 模式）\n\n"
        else:
            auto_detect_msg = ""
        
        # 优化提示
        result = self.optimize(clean_prompt, target_ai, mode)
        
        # 格式化响应
        formatted_response = auto_detect_msg + self.format_response(result)
        
        # 检查是否需要进一步输入（DETAIL 模式的澄清问题）
        needs_input = bool(result.clarifying_questions)
        
        return formatted_response, needs_input
    
    def _extract_parameters(self, prompt: str) -> Tuple[Optional[TargetAI], Optional[OperatingMode]]:
        """从提示中提取目标 AI 和模式"""
        prompt_lower = prompt.lower()
        
        # 提取目标 AI
        target_ai = None
        if "chatgpt" in prompt_lower or "gpt" in prompt_lower:
            target_ai = TargetAI.CHATGPT
        elif "claude" in prompt_lower:
            target_ai = TargetAI.CLAUDE
        elif "gemini" in prompt_lower:
            target_ai = TargetAI.GEMINI
        elif "other" in prompt_lower:
            target_ai = TargetAI.OTHER
        
        # 提取模式
        mode = None
        if "detail" in prompt_lower:
            mode = OperatingMode.DETAIL
        elif "basic" in prompt_lower:
            mode = OperatingMode.BASIC
        
        return target_ai, mode
    
    def _clean_prompt(self, prompt: str) -> str:
        """清理提示，去除参数指示"""
        # 移除常见的参数模式
        patterns = [
            r'(detail|basic)\s+(using|with|使用)\s+(chatgpt|claude|gemini|other)\s*[-—]\s*',
            r'(chatgpt|claude|gemini|other)\s*[-—]\s*',
            r'(detail|basic)\s*[-—]\s*',
        ]
        
        cleaned = prompt
        for pattern in patterns:
            cleaned = re.sub(pattern, '', cleaned, flags=re.IGNORECASE)
        
        return cleaned.strip()
    
    def _auto_detect_mode(self, prompt: str) -> OperatingMode:
        """自动检测合适的模式"""
        # 简单的启发式：基于长度和复杂度
        complexity_indicators = [
            len(prompt) > 80,  # 降低长度阈值
            any(word in prompt for word in ["完整", "系统", "包括", "实现", "设计"]),
            any(word in prompt.lower() for word in ["multiple", "steps", "complex", "detailed"]),
            prompt.count(",") > 2 or prompt.count("、") > 2,  # 降低逗号阈值
            prompt.count(".") > 1 or prompt.count("。") > 1,
            any(word in prompt.lower() for word in ["create", "design", "implement", "develop"]),
            any(word in prompt for word in ["并", "以及", "还有"])  # 添加连接词检测
        ]
        
        complexity_score = sum(complexity_indicators)
        
        # 如果复杂度得分 >= 2，使用 DETAIL 模式（降低阈值）
        return OperatingMode.DETAIL if complexity_score >= 2 else OperatingMode.BASIC