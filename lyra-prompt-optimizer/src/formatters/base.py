"""格式化器基类"""

from abc import ABC, abstractmethod
from ..core.types import OptimizationResult


class FormatterBase(ABC):
    """所有格式化器的基类"""
    
    def __init__(self, name: str):
        self.name = name
    
    @abstractmethod
    def format(self, result: OptimizationResult) -> str:
        """格式化优化结果"""
        pass