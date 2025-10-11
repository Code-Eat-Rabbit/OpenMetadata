"""Lyra 系统的核心类型定义"""

from enum import Enum
from typing import List, Optional, Dict, Any
from dataclasses import dataclass


class TargetAI(Enum):
    """支持的 AI 平台"""
    CHATGPT = "ChatGPT"
    CLAUDE = "Claude"
    GEMINI = "Gemini"
    OTHER = "Other"


class OperatingMode(Enum):
    """操作模式"""
    DETAIL = "DETAIL"  # 详细模式：收集上下文，提出澄清问题
    BASIC = "BASIC"    # 基础模式：快速修复主要问题


class PromptType(Enum):
    """提示类型分类"""
    CREATIVE = "Creative"
    TECHNICAL = "Technical"
    EDUCATIONAL = "Educational"
    COMPLEX = "Complex"
    SIMPLE = "Simple"


class OptimizationTechnique(Enum):
    """优化技术"""
    ROLE_ASSIGNMENT = "Role Assignment"
    CONTEXT_LAYERING = "Context Layering"
    OUTPUT_SPECS = "Output Specifications"
    TASK_DECOMPOSITION = "Task Decomposition"
    CHAIN_OF_THOUGHT = "Chain of Thought"
    FEW_SHOT_LEARNING = "Few-shot Learning"
    MULTI_PERSPECTIVE = "Multi-perspective Analysis"
    CONSTRAINT_OPTIMIZATION = "Constraint Optimization"


@dataclass
class DeconstructResult:
    """解构阶段的结果"""
    core_intent: str
    key_entities: List[str]
    context: Dict[str, Any]
    output_requirements: List[str]
    constraints: List[str]
    missing_elements: List[str]


@dataclass
class DiagnoseResult:
    """诊断阶段的结果"""
    clarity_gaps: List[str]
    ambiguities: List[str]
    specificity_issues: List[str]
    completeness_score: float
    structure_needs: List[str]
    prompt_type: PromptType


@dataclass
class DevelopResult:
    """开发阶段的结果"""
    selected_techniques: List[OptimizationTechnique]
    assigned_role: str
    enhanced_context: str
    logical_structure: str
    platform_optimizations: Dict[str, Any]


@dataclass
class OptimizationResult:
    """优化结果"""
    original_prompt: str
    optimized_prompt: str
    key_improvements: List[str]
    techniques_applied: List[str]
    pro_tip: Optional[str]
    clarifying_questions: Optional[List[str]]
    complexity_level: str