#!/usr/bin/env python3
"""Lyra 快速开始脚本"""

from src.core import LyraOptimizer


def main():
    """主函数"""
    # 创建优化器实例
    optimizer = LyraOptimizer()
    
    # 显示欢迎消息
    print(optimizer.get_welcome_message())
    print("\n" + "="*80 + "\n")
    
    # 交互式会话
    print("💡 提示：输入 'quit' 或 'exit' 退出程序")
    print("💡 提示：输入 'help' 查看更多示例\n")
    
    while True:
        # 获取用户输入
        user_input = input("您的提示 > ").strip()
        
        # 检查退出命令
        if user_input.lower() in ['quit', 'exit', 'q']:
            print("\n👋 感谢使用 Lyra！再见！")
            break
        
        # 检查帮助命令
        if user_input.lower() == 'help':
            print_help()
            continue
        
        # 空输入
        if not user_input:
            continue
        
        # 处理用户输入
        try:
            response, needs_clarification = optimizer.process_user_input(user_input)
            print("\n" + response)
            
            if needs_clarification:
                print("\n💬 提示：回答上述问题可以获得更精确的优化结果")
            
        except Exception as e:
            print(f"\n❌ 错误：{str(e)}")
            print("请尝试重新输入或查看 'help' 获取示例")
        
        print("\n" + "-"*80 + "\n")


def print_help():
    """打印帮助信息"""
    help_text = """
📖 帮助信息
============

使用格式：
  [模式] 使用 [AI平台] — [您的提示]

模式选项：
  • DETAIL - 详细模式，会提出澄清问题
  • BASIC  - 基础模式，快速优化

AI 平台选项：
  • ChatGPT
  • Claude  
  • Gemini
  • Other

示例：
  1. DETAIL 使用 ChatGPT — 写一篇关于人工智能的博客文章
  2. BASIC 使用 Claude — 解释递归算法
  3. 创建一个产品发布计划（自动检测模式）

特殊命令：
  • help - 显示此帮助信息
  • quit/exit - 退出程序
"""
    print(help_text)


if __name__ == "__main__":
    print("🎯 Lyra AI 提示优化专家 v1.0")
    print("="*80)
    
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n👋 程序已中断。再见！")
    except Exception as e:
        print(f"\n❌ 发生错误：{str(e)}")
        print("请检查输入并重试。")