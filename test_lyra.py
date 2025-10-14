#!/usr/bin/env python3
"""
Test script for Lyra AI Prompt Optimizer
Demonstrates various optimization scenarios and validates the 4-D methodology.
"""

from lyra_prompt_optimizer import LyraPromptOptimizer


def test_lyra_examples():
    """Test Lyra with various example prompts."""
    lyra = LyraPromptOptimizer()
    
    # Test cases covering different scenarios
    test_cases = [
        {
            "name": "Simple Creative Request",
            "input": "BASIC using ChatGPT — Write me a marketing email",
            "expected_features": ["role_assignment", "output_specs"]
        },
        {
            "name": "Complex Technical Request", 
            "input": "DETAIL using Claude — Help me optimize my Python code for better performance",
            "expected_features": ["constraint_optimization", "task_decomposition"]
        },
        {
            "name": "Educational Request",
            "input": "BASIC using Gemini — Explain machine learning to beginners",
            "expected_features": ["few_shot_learning", "output_specs"]
        },
        {
            "name": "Vague Request (Auto-detect)",
            "input": "help with my resume",
            "expected_features": ["role_assignment", "context_layering"]
        },
        {
            "name": "Complex Analysis Request",
            "input": "DETAIL using Claude — Analyze the impact of AI on job markets, considering economic, social, and technological factors",
            "expected_features": ["chain_of_thought", "multi_perspective", "constraint_optimization"]
        }
    ]
    
    print("🚀 Testing Lyra AI Prompt Optimizer")
    print("=" * 60)
    
    for i, test_case in enumerate(test_cases, 1):
        print(f"\n📝 Test Case {i}: {test_case['name']}")
        print(f"Input: {test_case['input']}")
        print("-" * 40)
        
        try:
            result = lyra.optimize_prompt(test_case['input'])
            print(result)
            
            # Basic validation
            if "Your Optimized Prompt:" in result:
                print("✅ Successfully generated optimized prompt")
            else:
                print("❌ Failed to generate optimized prompt")
                
        except Exception as e:
            print(f"❌ Error: {e}")
        
        print("=" * 60)


def test_4d_methodology():
    """Test the 4-D methodology components individually."""
    lyra = LyraPromptOptimizer()
    
    print("\n🔍 Testing 4-D Methodology Components")
    print("=" * 60)
    
    test_prompt = "Write a blog post about sustainable living"
    
    # Test Deconstruct
    print("1. DECONSTRUCT:")
    analysis = lyra.deconstruct(test_prompt)
    print(f"   Core Intent: {analysis['core_intent']}")
    print(f"   Key Entities: {analysis['key_entities']}")
    print(f"   Missing Elements: {analysis['missing_elements']}")
    
    # Test Diagnose
    print("\n2. DIAGNOSE:")
    diagnosis = lyra.diagnose(analysis)
    print(f"   Clarity Issues: {diagnosis['clarity_issues']}")
    print(f"   Specificity Level: {diagnosis['specificity_level']}")
    print(f"   Completeness Score: {diagnosis['completeness_score']}%")
    print(f"   Request Type: {diagnosis['request_type'].value}")
    
    # Test Develop
    print("\n3. DEVELOP:")
    from lyra_prompt_optimizer import AIPlatform
    development_plan = lyra.develop(analysis, diagnosis, AIPlatform.CHATGPT)
    print(f"   Selected Techniques: {development_plan['selected_techniques']}")
    print(f"   AI Role: {development_plan['ai_role']}")
    print(f"   Structure: {development_plan['structure']}")
    
    # Test Deliver
    print("\n4. DELIVER:")
    result = lyra.deliver(test_prompt, analysis, diagnosis, development_plan, AIPlatform.CHATGPT)
    print(f"   Optimized Prompt Length: {len(result.optimized_prompt)} characters")
    print(f"   Improvements Made: {len(result.improvements)}")
    print(f"   Techniques Applied: {result.techniques_applied}")
    
    print("=" * 60)


def demonstrate_platform_differences():
    """Demonstrate how optimization differs across AI platforms."""
    lyra = LyraPromptOptimizer()
    
    print("\n🤖 Platform-Specific Optimization Demo")
    print("=" * 60)
    
    base_prompt = "Create a comprehensive guide for starting a small business"
    
    platforms = ["ChatGPT", "Claude", "Gemini", "Other"]
    
    for platform in platforms:
        print(f"\n📱 Optimizing for {platform}:")
        print("-" * 30)
        
        test_input = f"DETAIL using {platform} — {base_prompt}"
        
        try:
            result = lyra.optimize_prompt(test_input)
            # Extract just the optimized prompt part for comparison
            prompt_start = result.find("**Your Optimized Prompt:**\n") + len("**Your Optimized Prompt:**\n")
            prompt_end = result.find("\n\n**Key Improvements:**")
            if prompt_end == -1:
                prompt_end = result.find("\n\n**What Changed:**")
            
            optimized_prompt = result[prompt_start:prompt_end].strip()
            print(f"Length: {len(optimized_prompt)} characters")
            print(f"Preview: {optimized_prompt[:150]}...")
            
        except Exception as e:
            print(f"❌ Error: {e}")
    
    print("=" * 60)


if __name__ == "__main__":
    # Run all tests
    test_lyra_examples()
    test_4d_methodology()
    demonstrate_platform_differences()
    
    print("\n🎉 Testing Complete!")
    print("\nTo run Lyra interactively, use:")
    print("python lyra_prompt_optimizer.py")