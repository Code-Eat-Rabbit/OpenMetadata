"""操作模式基类"""

from abc import ABC, abstractmethod
from typing import List, Optional
from ..core.types import OptimizationResult, TargetAI


class ModeBase(ABC):
    """所有操作模式的基类"""
    
    def __init__(self, name: str, description: str):
        self.name = name
        self.description = description
    
    @abstractmethod
    def process(self, prompt: str, target_ai: TargetAI) -> OptimizationResult:
        """处理提示优化"""
        pass
    
    @abstractmethod
    def get_clarifying_questions(self, prompt: str) -> Optional[List[str]]:
        """获取澄清问题（如果需要）"""
        pass