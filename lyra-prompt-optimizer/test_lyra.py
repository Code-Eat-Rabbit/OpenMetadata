#!/usr/bin/env python3
"""Lyra 测试脚本"""

import sys
from src.core import LyraOptimizer, TargetAI, OperatingMode


def test_basic_functionality():
    """测试基本功能"""
    print("🧪 测试基本功能...")
    
    optimizer = LyraOptimizer()
    
    # 测试欢迎消息
    assert optimizer.get_welcome_message() is not None
    print("✅ 欢迎消息测试通过")
    
    # 测试 BASIC 模式
    result = optimizer.optimize(
        "写一封邮件",
        TargetAI.CHATGPT,
        OperatingMode.BASIC
    )
    assert result.optimized_prompt is not None
    assert len(result.key_improvements) > 0
    print("✅ BASIC 模式测试通过")
    
    # 测试 DETAIL 模式
    result = optimizer.optimize(
        "创建一个复杂的系统架构",
        TargetAI.CLAUDE,
        OperatingMode.DETAIL
    )
    assert result.optimized_prompt is not None
    assert result.clarifying_questions is not None
    print("✅ DETAIL 模式测试通过")


def test_auto_detection():
    """测试自动检测功能"""
    print("\n🧪 测试自动检测功能...")
    
    optimizer = LyraOptimizer()
    
    # 简单提示应该使用 BASIC 模式
    response, _ = optimizer.process_user_input("什么是 Python")
    # 检查是否为基础复杂度
    result = optimizer.optimize("什么是 Python", TargetAI.OTHER, None)
    assert result.complexity_level == "基础"
    print("✅ 简单提示自动检测通过")
    
    # 复杂提示应该使用 DETAIL 模式
    complex_prompt = "设计并实现一个完整的电子商务平台，包括用户管理、产品目录、购物车、支付系统和订单跟踪"
    response, _ = optimizer.process_user_input(complex_prompt)
    # 检查是否为详细复杂度
    result = optimizer.optimize(complex_prompt, TargetAI.OTHER, None)
    assert result.complexity_level == "详细"
    print("✅ 复杂提示自动检测通过")


def test_parameter_extraction():
    """测试参数提取功能"""
    print("\n🧪 测试参数提取功能...")
    
    optimizer = LyraOptimizer()
    
    # 测试各种输入格式
    test_cases = [
        ("DETAIL 使用 ChatGPT — 写文章", True),
        ("BASIC using Claude - help with code", True),
        ("使用 Gemini 创建内容", True),
        ("just a simple prompt", False)
    ]
    
    for input_text, should_extract in test_cases:
        response, _ = optimizer.process_user_input(input_text)
        assert response is not None
    
    print("✅ 参数提取测试通过")


def test_all_platforms():
    """测试所有平台"""
    print("\n🧪 测试所有 AI 平台...")
    
    optimizer = LyraOptimizer()
    prompt = "解释机器学习"
    
    for platform in [TargetAI.CHATGPT, TargetAI.CLAUDE, TargetAI.GEMINI, TargetAI.OTHER]:
        result = optimizer.optimize(prompt, platform, OperatingMode.BASIC)
        assert result.optimized_prompt is not None
        print(f"✅ {platform.value} 平台测试通过")


def test_formatting():
    """测试格式化功能"""
    print("\n🧪 测试响应格式化...")
    
    optimizer = LyraOptimizer()
    
    # 测试简单格式
    simple_result = optimizer.optimize(
        "你好",
        TargetAI.CHATGPT,
        OperatingMode.BASIC
    )
    simple_formatted = optimizer.format_response(simple_result)
    assert "**您的优化提示：**" in simple_formatted
    print("✅ 简单格式化测试通过")
    
    # 测试复杂格式
    complex_result = optimizer.optimize(
        "创建一个完整的项目管理系统",
        TargetAI.CLAUDE,
        OperatingMode.DETAIL
    )
    complex_formatted = optimizer.format_response(complex_result)
    assert "**关键改进：**" in complex_formatted or "**您的优化提示：**" in complex_formatted
    print("✅ 复杂格式化测试通过")


def test_edge_cases():
    """测试边缘情况"""
    print("\n🧪 测试边缘情况...")
    
    optimizer = LyraOptimizer()
    
    # 空输入
    response, needs_input = optimizer.process_user_input("")
    assert needs_input is True
    print("✅ 空输入处理通过")
    
    # 超长输入
    long_prompt = "这是一个测试 " * 100
    result = optimizer.optimize(long_prompt, TargetAI.CHATGPT, OperatingMode.BASIC)
    assert result is not None
    print("✅ 长输入处理通过")
    
    # 特殊字符
    special_prompt = "创建一个包含 @#$% 特殊字符的内容"
    result = optimizer.optimize(special_prompt, TargetAI.CHATGPT, OperatingMode.BASIC)
    assert result is not None
    print("✅ 特殊字符处理通过")


def run_all_tests():
    """运行所有测试"""
    print("🚀 开始运行 Lyra 测试套件\n")
    
    try:
        test_basic_functionality()
        test_auto_detection()
        test_parameter_extraction()
        test_all_platforms()
        test_formatting()
        test_edge_cases()
        
        print("\n✅ 所有测试通过！Lyra 系统运行正常。")
        return True
        
    except AssertionError as e:
        print(f"\n❌ 测试失败：{str(e)}")
        return False
    except Exception as e:
        print(f"\n❌ 发生错误：{str(e)}")
        return False


if __name__ == "__main__":
    success = run_all_tests()
    sys.exit(0 if success else 1)