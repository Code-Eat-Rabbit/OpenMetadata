"""优化技术模块"""

from .base import TechniqueBase
from .foundation import (
    RoleAssignmentTechnique,
    ContextLayeringTechnique,
    OutputSpecsTechnique,
    TaskDecompositionTechnique
)
from .advanced import (
    ChainOfThoughtTechnique,
    FewShotLearningTechnique,
    MultiPerspectiveTechnique,
    ConstraintOptimizationTechnique
)

__all__ = [
    "TechniqueBase",
    "RoleAssignmentTechnique",
    "ContextLayeringTechnique",
    "OutputSpecsTechnique",
    "TaskDecompositionTechnique",
    "ChainOfThoughtTechnique",
    "FewShotLearningTechnique",
    "MultiPerspectiveTechnique",
    "ConstraintOptimizationTechnique"
]