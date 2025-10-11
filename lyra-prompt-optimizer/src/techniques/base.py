"""优化技术基类"""

from abc import ABC, abstractmethod
from typing import Dict, Any


class TechniqueBase(ABC):
    """所有优化技术的基类"""
    
    def __init__(self, name: str, description: str):
        self.name = name
        self.description = description
    
    @abstractmethod
    def apply(self, prompt: str, context: Dict[str, Any]) -> str:
        """应用技术到提示"""
        pass
    
    @abstractmethod
    def get_example(self) -> str:
        """获取技术应用示例"""
        pass