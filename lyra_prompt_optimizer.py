#!/usr/bin/env python3
"""
Lyra - AI Prompt Optimization Specialist
Transform any user input into precision-crafted prompts using the 4-D methodology.
"""

import re
import json
from typing import Dict, List, Tuple, Optional, Any
from dataclasses import dataclass
from enum import Enum


class AIPlatform(Enum):
    CHATGPT = "ChatGPT"
    CLAUDE = "Claude"
    GEMINI = "Gemini"
    OTHER = "Other"


class OptimizationMode(Enum):
    DETAIL = "DETAIL"
    BASIC = "BASIC"


class RequestType(Enum):
    CREATIVE = "Creative"
    TECHNICAL = "Technical"
    EDUCATIONAL = "Educational"
    COMPLEX = "Complex"


@dataclass
class OptimizationRequest:
    original_prompt: str
    platform: AIPlatform
    mode: OptimizationMode
    context: Dict[str, Any] = None


@dataclass
class OptimizationResult:
    optimized_prompt: str
    improvements: List[str]
    techniques_applied: List[str]
    pro_tip: str = ""


class LyraPromptOptimizer:
    """
    Master-level AI prompt optimization specialist implementing the 4-D methodology:
    Deconstruct → Diagnose → Develop → Deliver
    """
    
    def __init__(self):
        self.optimization_techniques = {
            "role_assignment": "Assign specific AI role/expertise",
            "context_layering": "Add structured context and background",
            "output_specs": "Define clear output requirements",
            "task_decomposition": "Break complex tasks into steps",
            "chain_of_thought": "Add reasoning frameworks",
            "few_shot_learning": "Provide examples",
            "multi_perspective": "Multiple viewpoint analysis",
            "constraint_optimization": "Add specific constraints and parameters"
        }
        
        self.platform_optimizations = {
            AIPlatform.CHATGPT: {
                "features": ["structured_sections", "conversation_starters", "system_messages"],
                "max_context": "moderate",
                "strengths": ["dialogue", "creative_writing", "general_tasks"]
            },
            AIPlatform.CLAUDE: {
                "features": ["longer_context", "reasoning_frameworks", "detailed_analysis"],
                "max_context": "very_high",
                "strengths": ["analysis", "reasoning", "complex_tasks"]
            },
            AIPlatform.GEMINI: {
                "features": ["creative_tasks", "comparative_analysis", "multimodal"],
                "max_context": "high",
                "strengths": ["creativity", "comparison", "visual_tasks"]
            },
            AIPlatform.OTHER: {
                "features": ["universal_best_practices"],
                "max_context": "moderate",
                "strengths": ["general_purpose"]
            }
        }
    
    def display_welcome_message(self) -> str:
        """Display the required welcome message."""
        return """Hello! I'm Lyra, your AI prompt optimizer. I transform vague requests into precise, effective prompts that deliver better results.

**What I need to know:**
- **Target AI:** ChatGPT, Claude, Gemini, or Other
- **Prompt Style:** DETAIL (I'll ask clarifying questions first) or BASIC (quick optimization)

**Examples:**
- "DETAIL using ChatGPT — Write me a marketing email"
- "BASIC using Claude — Help with my resume"

Just share your rough prompt and I'll handle the optimization!"""
    
    def parse_user_input(self, user_input: str) -> OptimizationRequest:
        """Parse user input to extract platform, mode, and prompt."""
        # Extract mode (DETAIL or BASIC)
        mode_match = re.search(r'\b(DETAIL|BASIC)\b', user_input, re.IGNORECASE)
        mode = OptimizationMode.DETAIL if mode_match and mode_match.group(1).upper() == "DETAIL" else OptimizationMode.BASIC
        
        # Extract platform
        platform = AIPlatform.OTHER  # default
        for ai_platform in AIPlatform:
            if ai_platform.value.lower() in user_input.lower():
                platform = ai_platform
                break
        
        # Extract the actual prompt (remove mode and platform indicators)
        prompt = user_input
        if mode_match:
            prompt = prompt.replace(mode_match.group(0), "")
        
        # Remove platform mentions
        for ai_platform in AIPlatform:
            prompt = re.sub(rf'\busing\s+{ai_platform.value}\b', '', prompt, flags=re.IGNORECASE)
            prompt = re.sub(rf'\b{ai_platform.value}\b', '', prompt, flags=re.IGNORECASE)
        
        # Clean up the prompt
        prompt = re.sub(r'[—-]+', '', prompt).strip()
        
        return OptimizationRequest(
            original_prompt=prompt,
            platform=platform,
            mode=mode
        )
    
    def deconstruct(self, prompt: str) -> Dict[str, Any]:
        """Step 1: Extract core intent, key entities, and context."""
        analysis = {
            "core_intent": self._extract_core_intent(prompt),
            "key_entities": self._extract_key_entities(prompt),
            "context_provided": self._assess_context(prompt),
            "output_requirements": self._identify_output_requirements(prompt),
            "constraints": self._identify_constraints(prompt),
            "missing_elements": []
        }
        
        # Identify what's missing
        if not analysis["output_requirements"]:
            analysis["missing_elements"].append("output_format")
        if not analysis["context_provided"]:
            analysis["missing_elements"].append("context")
        if not analysis["constraints"]:
            analysis["missing_elements"].append("constraints")
            
        return analysis
    
    def diagnose(self, analysis: Dict[str, Any]) -> Dict[str, Any]:
        """Step 2: Audit for clarity gaps and assess structure needs."""
        diagnosis = {
            "clarity_issues": [],
            "specificity_level": "low",
            "completeness_score": 0,
            "complexity_level": self._assess_complexity(analysis),
            "request_type": self._determine_request_type(analysis)
        }
        
        # Check for clarity issues
        if not analysis["core_intent"]:
            diagnosis["clarity_issues"].append("unclear_intent")
        if len(analysis["key_entities"]) == 0:
            diagnosis["clarity_issues"].append("missing_entities")
        if len(analysis["missing_elements"]) > 2:
            diagnosis["clarity_issues"].append("insufficient_detail")
        
        # Assess specificity
        if analysis["output_requirements"] and analysis["constraints"]:
            diagnosis["specificity_level"] = "high"
        elif analysis["output_requirements"] or analysis["constraints"]:
            diagnosis["specificity_level"] = "medium"
        
        # Calculate completeness score
        total_elements = 5  # intent, entities, context, output, constraints
        provided_elements = sum([
            bool(analysis["core_intent"]),
            bool(analysis["key_entities"]),
            bool(analysis["context_provided"]),
            bool(analysis["output_requirements"]),
            bool(analysis["constraints"])
        ])
        diagnosis["completeness_score"] = (provided_elements / total_elements) * 100
        
        return diagnosis
    
    def develop(self, analysis: Dict[str, Any], diagnosis: Dict[str, Any], platform: AIPlatform) -> Dict[str, Any]:
        """Step 3: Select optimal techniques and enhance the prompt."""
        development_plan = {
            "selected_techniques": [],
            "ai_role": "",
            "enhanced_context": "",
            "structure": [],
            "platform_adaptations": []
        }
        
        request_type = diagnosis["request_type"]
        
        # Select techniques based on request type
        if request_type == RequestType.CREATIVE:
            development_plan["selected_techniques"].extend([
                "multi_perspective", "role_assignment", "context_layering"
            ])
            development_plan["ai_role"] = "creative writing expert"
        elif request_type == RequestType.TECHNICAL:
            development_plan["selected_techniques"].extend([
                "constraint_optimization", "task_decomposition", "output_specs"
            ])
            development_plan["ai_role"] = "technical specialist"
        elif request_type == RequestType.EDUCATIONAL:
            development_plan["selected_techniques"].extend([
                "few_shot_learning", "chain_of_thought", "output_specs"
            ])
            development_plan["ai_role"] = "educational expert"
        elif request_type == RequestType.COMPLEX:
            development_plan["selected_techniques"].extend([
                "chain_of_thought", "task_decomposition", "constraint_optimization"
            ])
            development_plan["ai_role"] = "analytical expert"
        
        # Add platform-specific adaptations
        platform_config = self.platform_optimizations.get(platform, {})
        development_plan["platform_adaptations"] = platform_config.get("features", [])
        
        # Enhance context based on missing elements
        if "context" in analysis["missing_elements"]:
            development_plan["enhanced_context"] = "Add relevant background information and context"
        
        # Define structure based on complexity
        if diagnosis["complexity_level"] == "high":
            development_plan["structure"] = [
                "role_definition", "context_section", "task_breakdown", 
                "output_specifications", "examples", "constraints"
            ]
        else:
            development_plan["structure"] = [
                "role_definition", "clear_task", "output_format"
            ]
        
        return development_plan
    
    def deliver(self, original_prompt: str, analysis: Dict[str, Any], 
               diagnosis: Dict[str, Any], development_plan: Dict[str, Any],
               platform: AIPlatform) -> OptimizationResult:
        """Step 4: Construct the optimized prompt."""
        
        # Build the optimized prompt
        optimized_sections = []
        
        # Add role assignment
        if development_plan["ai_role"]:
            role_section = f"You are a {development_plan['ai_role']}."
            optimized_sections.append(role_section)
        
        # Add enhanced context if needed
        if development_plan["enhanced_context"]:
            context_section = f"Context: {development_plan['enhanced_context']}"
            optimized_sections.append(context_section)
        
        # Add the main task with improvements
        task_section = self._enhance_task_description(original_prompt, analysis, diagnosis)
        optimized_sections.append(task_section)
        
        # Add output specifications
        if "output_specs" in development_plan["selected_techniques"]:
            output_section = self._generate_output_specifications(analysis, diagnosis)
            if output_section:
                optimized_sections.append(output_section)
        
        # Add examples if using few-shot learning
        if "few_shot_learning" in development_plan["selected_techniques"]:
            examples_section = self._generate_examples(diagnosis["request_type"])
            if examples_section:
                optimized_sections.append(examples_section)
        
        # Add constraints
        if "constraint_optimization" in development_plan["selected_techniques"]:
            constraints_section = self._generate_constraints(analysis, platform)
            if constraints_section:
                optimized_sections.append(constraints_section)
        
        # Combine sections
        optimized_prompt = "\n\n".join(optimized_sections)
        
        # Apply platform-specific formatting
        optimized_prompt = self._apply_platform_formatting(optimized_prompt, platform)
        
        # Generate improvements list
        improvements = self._generate_improvements_list(analysis, diagnosis, development_plan)
        
        # Generate pro tip
        pro_tip = self._generate_pro_tip(platform, diagnosis["request_type"])
        
        return OptimizationResult(
            optimized_prompt=optimized_prompt,
            improvements=improvements,
            techniques_applied=development_plan["selected_techniques"],
            pro_tip=pro_tip
        )
    
    def optimize_prompt(self, user_input: str) -> str:
        """Main optimization method implementing the 4-D methodology."""
        # Parse the user input
        request = self.parse_user_input(user_input)
        
        # Auto-detect complexity if not specified
        complexity = self._assess_complexity_from_prompt(request.original_prompt)
        if complexity == "high" and request.mode == OptimizationMode.BASIC:
            override_message = f"\n**Note:** Detected complex task. Consider using DETAIL mode for better results. Proceeding with BASIC optimization.\n"
        else:
            override_message = ""
        
        # Apply 4-D methodology
        analysis = self.deconstruct(request.original_prompt)
        diagnosis = self.diagnose(analysis)
        development_plan = self.develop(analysis, diagnosis, request.platform)
        result = self.deliver(request.original_prompt, analysis, diagnosis, development_plan, request.platform)
        
        # Format response based on complexity
        if diagnosis["complexity_level"] == "high" or request.mode == OptimizationMode.DETAIL:
            return self._format_complex_response(result, override_message)
        else:
            return self._format_simple_response(result, override_message)
    
    # Helper methods
    def _extract_core_intent(self, prompt: str) -> str:
        """Extract the main intent from the prompt."""
        # Simple keyword-based intent extraction
        intent_keywords = {
            "write": "content creation",
            "create": "content creation", 
            "generate": "content generation",
            "analyze": "analysis",
            "explain": "explanation",
            "help": "assistance",
            "review": "review/feedback",
            "improve": "improvement",
            "optimize": "optimization"
        }
        
        prompt_lower = prompt.lower()
        for keyword, intent in intent_keywords.items():
            if keyword in prompt_lower:
                return intent
        
        return "general assistance"
    
    def _extract_key_entities(self, prompt: str) -> List[str]:
        """Extract key entities from the prompt."""
        # Simple entity extraction - look for nouns and important terms
        entities = []
        words = prompt.split()
        
        # Look for capitalized words (proper nouns)
        for word in words:
            if word[0].isupper() and len(word) > 2:
                entities.append(word)
        
        # Look for common entity patterns
        entity_patterns = [
            r'\b(email|resume|report|article|blog|story|code|script|function)\b',
            r'\b(marketing|sales|technical|business|academic)\b',
            r'\b(company|product|service|website|app|software)\b'
        ]
        
        for pattern in entity_patterns:
            matches = re.findall(pattern, prompt, re.IGNORECASE)
            entities.extend(matches)
        
        return list(set(entities))  # Remove duplicates
    
    def _assess_context(self, prompt: str) -> bool:
        """Assess if sufficient context is provided."""
        context_indicators = [
            "for", "about", "regarding", "concerning", "related to",
            "background", "context", "situation", "scenario"
        ]
        
        return any(indicator in prompt.lower() for indicator in context_indicators)
    
    def _identify_output_requirements(self, prompt: str) -> List[str]:
        """Identify specified output requirements."""
        requirements = []
        
        format_patterns = [
            r'\b(format|structure|style|tone|length)\b',
            r'\b(bullet points|numbered list|paragraph|essay)\b',
            r'\b(formal|informal|professional|casual)\b',
            r'\b(short|long|detailed|brief|comprehensive)\b'
        ]
        
        for pattern in format_patterns:
            matches = re.findall(pattern, prompt, re.IGNORECASE)
            requirements.extend(matches)
        
        return requirements
    
    def _identify_constraints(self, prompt: str) -> List[str]:
        """Identify constraints in the prompt."""
        constraints = []
        
        constraint_patterns = [
            r'\b(must|should|need to|required|limit|maximum|minimum)\b',
            r'\b(avoid|don\'t|cannot|shouldn\'t)\b',
            r'\b(within \d+|under \d+|at least \d+)\b'
        ]
        
        for pattern in constraint_patterns:
            matches = re.findall(pattern, prompt, re.IGNORECASE)
            constraints.extend(matches)
        
        return constraints
    
    def _assess_complexity(self, analysis: Dict[str, Any]) -> str:
        """Assess the complexity level of the request."""
        complexity_score = 0
        
        # Factors that increase complexity
        if len(analysis["key_entities"]) > 3:
            complexity_score += 1
        if len(analysis["missing_elements"]) > 2:
            complexity_score += 1
        if analysis["core_intent"] in ["analysis", "optimization", "complex reasoning"]:
            complexity_score += 2
        if len(analysis["constraints"]) > 2:
            complexity_score += 1
        
        if complexity_score >= 3:
            return "high"
        elif complexity_score >= 1:
            return "medium"
        else:
            return "low"
    
    def _assess_complexity_from_prompt(self, prompt: str) -> str:
        """Quick complexity assessment from raw prompt."""
        if len(prompt.split()) > 50 or "analyze" in prompt.lower() or "complex" in prompt.lower():
            return "high"
        elif len(prompt.split()) > 20:
            return "medium"
        else:
            return "low"
    
    def _determine_request_type(self, analysis: Dict[str, Any]) -> RequestType:
        """Determine the type of request."""
        intent = analysis["core_intent"].lower()
        entities = [e.lower() for e in analysis["key_entities"]]
        
        creative_indicators = ["story", "creative", "blog", "marketing", "content creation"]
        technical_indicators = ["code", "script", "function", "technical", "analysis"]
        educational_indicators = ["explain", "teach", "learn", "educational", "tutorial"]
        
        if any(indicator in intent or any(indicator in entity for entity in entities) 
               for indicator in creative_indicators):
            return RequestType.CREATIVE
        elif any(indicator in intent or any(indicator in entity for entity in entities) 
                 for indicator in technical_indicators):
            return RequestType.TECHNICAL
        elif any(indicator in intent or any(indicator in entity for entity in entities) 
                 for indicator in educational_indicators):
            return RequestType.EDUCATIONAL
        else:
            return RequestType.COMPLEX
    
    def _enhance_task_description(self, original_prompt: str, analysis: Dict[str, Any], 
                                 diagnosis: Dict[str, Any]) -> str:
        """Enhance the original task description."""
        enhanced = f"Task: {original_prompt}"
        
        # Add clarity if needed
        if "unclear_intent" in diagnosis["clarity_issues"]:
            enhanced += f"\n\nObjective: {analysis['core_intent']}"
        
        # Add specificity
        if diagnosis["specificity_level"] == "low":
            enhanced += "\n\nPlease provide a detailed and specific response."
        
        return enhanced
    
    def _generate_output_specifications(self, analysis: Dict[str, Any], 
                                      diagnosis: Dict[str, Any]) -> str:
        """Generate output specifications section."""
        if analysis["output_requirements"]:
            return f"Output Requirements:\n- " + "\n- ".join(analysis["output_requirements"])
        else:
            # Add default output specs based on request type
            if diagnosis["request_type"] == RequestType.CREATIVE:
                return "Output Requirements:\n- Engaging and creative tone\n- Well-structured content\n- Clear and compelling language"
            elif diagnosis["request_type"] == RequestType.TECHNICAL:
                return "Output Requirements:\n- Precise and accurate information\n- Step-by-step approach when applicable\n- Technical clarity"
            else:
                return "Output Requirements:\n- Clear and well-organized response\n- Comprehensive coverage of the topic\n- Professional tone"
    
    def _generate_examples(self, request_type: RequestType) -> str:
        """Generate examples section for few-shot learning."""
        examples = {
            RequestType.CREATIVE: "Examples of good creative content:\n- Engaging opening lines\n- Vivid descriptions\n- Compelling narratives",
            RequestType.TECHNICAL: "Examples of good technical explanations:\n- Clear step-by-step instructions\n- Relevant code snippets\n- Practical applications",
            RequestType.EDUCATIONAL: "Examples of good educational content:\n- Clear explanations with examples\n- Progressive complexity\n- Interactive elements",
            RequestType.COMPLEX: "Examples of good analysis:\n- Structured reasoning\n- Evidence-based conclusions\n- Multiple perspectives"
        }
        
        return examples.get(request_type, "")
    
    def _generate_constraints(self, analysis: Dict[str, Any], platform: AIPlatform) -> str:
        """Generate constraints section."""
        constraints = []
        
        # Add existing constraints
        if analysis["constraints"]:
            constraints.extend(analysis["constraints"])
        
        # Add platform-specific constraints
        platform_config = self.platform_optimizations.get(platform, {})
        if platform_config.get("max_context") == "moderate":
            constraints.append("Keep response concise and focused")
        elif platform_config.get("max_context") == "very_high":
            constraints.append("Provide comprehensive and detailed analysis")
        
        if constraints:
            return "Constraints:\n- " + "\n- ".join(constraints)
        
        return ""
    
    def _apply_platform_formatting(self, prompt: str, platform: AIPlatform) -> str:
        """Apply platform-specific formatting."""
        platform_config = self.platform_optimizations.get(platform, {})
        features = platform_config.get("features", [])
        
        if "structured_sections" in features:
            # Add clear section headers for ChatGPT
            sections = prompt.split("\n\n")
            formatted_sections = []
            for i, section in enumerate(sections):
                if i == 0 and not section.startswith("##"):
                    formatted_sections.append(f"## Role\n{section}")
                elif not section.startswith("##") and ":" in section:
                    title = section.split(":")[0]
                    content = ":".join(section.split(":")[1:])
                    formatted_sections.append(f"## {title}\n{content.strip()}")
                else:
                    formatted_sections.append(section)
            return "\n\n".join(formatted_sections)
        
        return prompt
    
    def _generate_improvements_list(self, analysis: Dict[str, Any], 
                                   diagnosis: Dict[str, Any], 
                                   development_plan: Dict[str, Any]) -> List[str]:
        """Generate list of improvements made."""
        improvements = []
        
        if development_plan["ai_role"]:
            improvements.append(f"Added specific AI role: {development_plan['ai_role']}")
        
        if "context" in analysis["missing_elements"]:
            improvements.append("Enhanced context and background information")
        
        if diagnosis["specificity_level"] == "low":
            improvements.append("Increased specificity and clarity")
        
        if "output_specs" in development_plan["selected_techniques"]:
            improvements.append("Added clear output specifications")
        
        if "few_shot_learning" in development_plan["selected_techniques"]:
            improvements.append("Included examples for better guidance")
        
        if "constraint_optimization" in development_plan["selected_techniques"]:
            improvements.append("Added relevant constraints and parameters")
        
        return improvements
    
    def _generate_pro_tip(self, platform: AIPlatform, request_type: RequestType) -> str:
        """Generate platform and request-specific pro tip."""
        tips = {
            (AIPlatform.CHATGPT, RequestType.CREATIVE): "Use conversation starters to guide ChatGPT's creative flow",
            (AIPlatform.CLAUDE, RequestType.COMPLEX): "Leverage Claude's reasoning capabilities with chain-of-thought prompts",
            (AIPlatform.GEMINI, RequestType.CREATIVE): "Take advantage of Gemini's multimodal capabilities for richer content",
            (AIPlatform.OTHER, RequestType.TECHNICAL): "Include specific examples to improve accuracy across different AI platforms"
        }
        
        return tips.get((platform, request_type), "Test your optimized prompt and iterate based on results")
    
    def _format_simple_response(self, result: OptimizationResult, override_message: str = "") -> str:
        """Format simple response for basic requests."""
        response = f"{override_message}**Your Optimized Prompt:**\n{result.optimized_prompt}\n\n"
        response += f"**What Changed:** {', '.join(result.improvements[:3])}"  # Show top 3 improvements
        
        return response
    
    def _format_complex_response(self, result: OptimizationResult, override_message: str = "") -> str:
        """Format complex response for detailed requests."""
        response = f"{override_message}**Your Optimized Prompt:**\n{result.optimized_prompt}\n\n"
        response += "**Key Improvements:**\n"
        for improvement in result.improvements:
            response += f"• {improvement}\n"
        
        response += f"\n**Techniques Applied:** {', '.join(result.techniques_applied)}\n\n"
        
        if result.pro_tip:
            response += f"**Pro Tip:** {result.pro_tip}"
        
        return response


def main():
    """Main function to run Lyra interactively."""
    lyra = LyraPromptOptimizer()
    
    print(lyra.display_welcome_message())
    print("\n" + "="*60 + "\n")
    
    while True:
        try:
            user_input = input("Enter your prompt (or 'quit' to exit): ").strip()
            
            if user_input.lower() in ['quit', 'exit', 'q']:
                print("Thanks for using Lyra! Happy prompting! 🚀")
                break
            
            if not user_input:
                print("Please enter a prompt to optimize.")
                continue
            
            # Optimize the prompt
            result = lyra.optimize_prompt(user_input)
            print("\n" + "="*60)
            print(result)
            print("="*60 + "\n")
            
        except KeyboardInterrupt:
            print("\n\nThanks for using Lyra! Happy prompting! 🚀")
            break
        except Exception as e:
            print(f"An error occurred: {e}")
            print("Please try again with a different prompt.")


if __name__ == "__main__":
    main()