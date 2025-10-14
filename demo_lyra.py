#!/usr/bin/env python3
"""
Lyra Demo Script - Showcase AI Prompt Optimization
Demonstrates the power of the 4-D methodology with real examples.
"""

from lyra_prompt_optimizer import LyraPromptOptimizer
import time


def print_header(title):
    """Print a formatted header."""
    print(f"\n{'='*60}")
    print(f"🎯 {title}")
    print(f"{'='*60}")


def print_separator():
    """Print a separator line."""
    print(f"\n{'-'*60}")


def demo_transformation(lyra, title, before, after_input):
    """Demonstrate a before/after transformation."""
    print_separator()
    print(f"📝 {title}")
    print(f"\n❌ BEFORE (Vague):")
    print(f'"{before}"')
    
    print(f"\n✅ AFTER (Lyra Optimized):")
    result = lyra.optimize_prompt(after_input)
    
    # Extract just the optimized prompt
    prompt_start = result.find("**Your Optimized Prompt:**\n") + len("**Your Optimized Prompt:**\n")
    prompt_end = result.find("\n\n**Key Improvements:**")
    if prompt_end == -1:
        prompt_end = result.find("\n\n**What Changed:**")
    
    optimized_prompt = result[prompt_start:prompt_end].strip()
    print(f'"{optimized_prompt}"')
    
    # Show improvements
    improvements_start = result.find("**Key Improvements:**")
    if improvements_start == -1:
        improvements_start = result.find("**What Changed:**")
    
    if improvements_start != -1:
        improvements_section = result[improvements_start:].split("\n\n")[0]
        print(f"\n💡 {improvements_section}")


def main():
    """Run the Lyra demonstration."""
    lyra = LyraPromptOptimizer()
    
    print_header("LYRA AI PROMPT OPTIMIZER DEMO")
    print("\n🚀 Welcome to Lyra - Transform vague requests into precision prompts!")
    print("\n📚 Using the revolutionary 4-D Methodology:")
    print("   1. DECONSTRUCT - Extract core intent and context")
    print("   2. DIAGNOSE - Identify clarity gaps and complexity")  
    print("   3. DEVELOP - Select optimal techniques and structure")
    print("   4. DELIVER - Construct the optimized prompt")
    
    # Demo 1: Creative Writing
    demo_transformation(
        lyra,
        "Creative Writing Enhancement",
        "write a blog post",
        "BASIC using ChatGPT — write a blog post"
    )
    
    # Demo 2: Technical Assistance  
    demo_transformation(
        lyra,
        "Technical Task Optimization",
        "help me with Python code",
        "DETAIL using Claude — help me with Python code"
    )
    
    # Demo 3: Educational Content
    demo_transformation(
        lyra,
        "Educational Content Structuring", 
        "explain machine learning",
        "BASIC using Gemini — explain machine learning"
    )
    
    # Demo 4: Business Analysis
    demo_transformation(
        lyra,
        "Complex Business Analysis",
        "analyze market trends",
        "DETAIL using Claude — analyze market trends for sustainable products in the next 5 years"
    )
    
    print_header("PLATFORM-SPECIFIC OPTIMIZATIONS")
    
    base_request = "Create a marketing strategy for a new product"
    platforms = [
        ("ChatGPT", "Structured sections, conversation flow"),
        ("Claude", "Detailed reasoning, comprehensive analysis"),
        ("Gemini", "Creative approaches, comparative insights"),
        ("Other", "Universal best practices")
    ]
    
    for platform, strength in platforms:
        print(f"\n🤖 {platform} Optimization:")
        print(f"   Strength: {strength}")
        
        result = lyra.optimize_prompt(f"BASIC using {platform} — {base_request}")
        
        # Show key techniques applied
        if "**Techniques Applied:**" in result:
            techniques_start = result.find("**Techniques Applied:**") + len("**Techniques Applied:**")
            techniques_end = result.find("\n\n", techniques_start)
            if techniques_end == -1:
                techniques_end = len(result)
            techniques = result[techniques_start:techniques_end].strip()
            print(f"   Techniques: {techniques}")
        
        # Show pro tip if available
        if "**Pro Tip:**" in result:
            tip_start = result.find("**Pro Tip:**") + len("**Pro Tip:**")
            tip = result[tip_start:].strip()
            print(f"   💡 Pro Tip: {tip}")
    
    print_header("INTERACTIVE FEATURES")
    
    print("\n🎛️  Lyra offers two optimization modes:")
    print("   • BASIC - Quick optimization for simple requests")
    print("   • DETAIL - Comprehensive optimization with clarifying questions")
    
    print("\n🎯 Auto-Detection Features:")
    print("   • Complexity assessment")
    print("   • Request type classification (Creative/Technical/Educational/Complex)")
    print("   • Platform-specific adaptations")
    print("   • Missing element identification")
    
    print("\n🔧 Advanced Techniques Available:")
    techniques = [
        "Role Assignment - Specific AI expertise",
        "Context Layering - Structured background",
        "Chain-of-Thought - Reasoning frameworks", 
        "Few-Shot Learning - Relevant examples",
        "Multi-Perspective - Multiple viewpoints",
        "Constraint Optimization - Specific parameters"
    ]
    
    for technique in techniques:
        print(f"   • {technique}")
    
    print_header("TRY LYRA YOURSELF")
    
    print("\n🚀 Ready to optimize your prompts?")
    print("\n   Run: python3 lyra_prompt_optimizer.py")
    print("\n   Example inputs:")
    print('   • "DETAIL using ChatGPT — Write me a marketing email"')
    print('   • "BASIC using Claude — Help with my resume"')
    print('   • "explain quantum computing to beginners"')
    
    print("\n📊 Or run comprehensive tests:")
    print("   Run: python3 test_lyra.py")
    
    print_separator()
    print("🎉 Thank you for exploring Lyra!")
    print("Transform your AI interactions with precision-crafted prompts! ✨")
    print_separator()


if __name__ == "__main__":
    main()