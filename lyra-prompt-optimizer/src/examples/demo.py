"""Lyra 优化器演示程序"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from src.core import LyraOptimizer, TargetAI, OperatingMode


def print_separator():
    """打印分隔线"""
    print("\n" + "="*80 + "\n")


def demo_welcome():
    """演示欢迎消息"""
    optimizer = LyraOptimizer()
    print(optimizer.get_welcome_message())
    print_separator()


def demo_basic_mode():
    """演示 BASIC 模式"""
    print("【演示 1：BASIC 模式 - 简单营销邮件】")
    
    optimizer = LyraOptimizer()
    
    # 用户输入
    user_input = "BASIC 使用 ChatGPT — 写一封营销邮件"
    print(f"用户输入：{user_input}")
    print()
    
    # 处理输入
    response, needs_input = optimizer.process_user_input(user_input)
    print(response)
    
    print_separator()


def demo_detail_mode():
    """演示 DETAIL 模式"""
    print("【演示 2：DETAIL 模式 - 复杂技术文档】")
    
    optimizer = LyraOptimizer()
    
    # 用户输入
    user_input = "DETAIL 使用 Claude — 创建一个关于微服务架构的技术文档，包括最佳实践、部署策略和示例代码"
    print(f"用户输入：{user_input}")
    print()
    
    # 处理输入
    response, needs_input = optimizer.process_user_input(user_input)
    print(response)
    
    print_separator()


def demo_auto_detection():
    """演示自动检测模式"""
    print("【演示 3：自动检测模式】")
    
    optimizer = LyraOptimizer()
    
    # 简单提示
    simple_prompt = "解释什么是机器学习"
    print(f"简单提示：{simple_prompt}")
    response, _ = optimizer.process_user_input(simple_prompt)
    print(response)
    
    print("\n" + "-"*40 + "\n")
    
    # 复杂提示
    complex_prompt = """设计一个完整的在线学习平台，包括用户管理系统、课程管理、
    视频播放功能、作业提交系统、讨论论坛和支付集成。需要考虑可扩展性、
    安全性和用户体验。"""
    
    print(f"复杂提示：{complex_prompt}")
    response, _ = optimizer.process_user_input(complex_prompt)
    print(response)
    
    print_separator()


def demo_different_platforms():
    """演示不同平台的优化"""
    print("【演示 4：不同平台优化对比】")
    
    optimizer = LyraOptimizer()
    prompt = "创建一个关于气候变化的演讲稿"
    
    platforms = [TargetAI.CHATGPT, TargetAI.CLAUDE, TargetAI.GEMINI]
    
    for platform in platforms:
        print(f"\n{platform.value} 优化版本：")
        print("-" * 40)
        
        result = optimizer.optimize(prompt, platform, OperatingMode.BASIC)
        formatted = optimizer.format_response(result)
        print(formatted)
        print()
    
    print_separator()


def demo_technique_showcase():
    """演示各种优化技术"""
    print("【演示 5：优化技术展示】")
    
    optimizer = LyraOptimizer()
    
    # 创意型提示
    creative_prompt = "写一个关于未来城市的科幻故事"
    print("创意型提示优化：")
    result = optimizer.optimize(creative_prompt, TargetAI.GEMINI, OperatingMode.DETAIL)
    print(optimizer.format_response(result))
    
    print("\n" + "-"*40 + "\n")
    
    # 技术型提示
    technical_prompt = "实现一个高效的排序算法"
    print("技术型提示优化：")
    result = optimizer.optimize(technical_prompt, TargetAI.CLAUDE, OperatingMode.DETAIL)
    print(optimizer.format_response(result))
    
    print_separator()


def interactive_demo():
    """交互式演示"""
    print("【交互式演示】")
    print("输入 'quit' 退出")
    print_separator()
    
    optimizer = LyraOptimizer()
    
    # 显示欢迎消息
    print(optimizer.get_welcome_message())
    print()
    
    while True:
        user_input = input("请输入您的提示（或 'quit' 退出）：").strip()
        
        if user_input.lower() == 'quit':
            print("感谢使用 Lyra！再见！")
            break
        
        if not user_input:
            continue
        
        response, needs_input = optimizer.process_user_input(user_input)
        print("\n" + response)
        
        if needs_input:
            print("\n如果您想回答上述问题以获得更精确的优化，请继续输入。")
        
        print("\n" + "-"*80 + "\n")


def main():
    """主函数"""
    print("🎯 Lyra AI 提示优化专家 - 演示程序")
    print_separator()
    
    # 运行各个演示
    demo_welcome()
    demo_basic_mode()
    demo_detail_mode()
    demo_auto_detection()
    demo_different_platforms()
    demo_technique_showcase()
    
    # 询问是否进入交互模式
    print("\n是否进入交互模式？(y/n)")
    choice = input().strip().lower()
    
    if choice == 'y':
        interactive_demo()


if __name__ == "__main__":
    main()