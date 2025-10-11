"""常见使用案例"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from src.core import LyraOptimizer, TargetAI, OperatingMode


class UseCaseExamples:
    """使用案例集合"""
    
    def __init__(self):
        self.optimizer = LyraOptimizer()
    
    def marketing_email_case(self):
        """营销邮件案例"""
        print("📧 使用案例：营销邮件")
        print("-" * 50)
        
        prompt = "写一封推广新产品的邮件"
        print(f"原始提示：{prompt}")
        
        result = self.optimizer.optimize(prompt, TargetAI.CHATGPT, OperatingMode.DETAIL)
        print(f"\n优化后：\n{result.optimized_prompt}")
        
        print("\n关键改进：")
        for improvement in result.key_improvements:
            print(f"  • {improvement}")
    
    def code_generation_case(self):
        """代码生成案例"""
        print("\n💻 使用案例：代码生成")
        print("-" * 50)
        
        prompt = "写一个排序函数"
        print(f"原始提示：{prompt}")
        
        result = self.optimizer.optimize(prompt, TargetAI.CLAUDE, OperatingMode.DETAIL)
        print(f"\n优化后：\n{result.optimized_prompt}")
        
        print("\n应用的技术：")
        for tech in result.techniques_applied:
            print(f"  • {tech}")
    
    def educational_content_case(self):
        """教育内容案例"""
        print("\n📚 使用案例：教育内容")
        print("-" * 50)
        
        prompt = "解释量子计算"
        print(f"原始提示：{prompt}")
        
        result = self.optimizer.optimize(prompt, TargetAI.GEMINI, OperatingMode.BASIC)
        print(f"\n优化后：\n{result.optimized_prompt}")
    
    def creative_writing_case(self):
        """创意写作案例"""
        print("\n✍️ 使用案例：创意写作")
        print("-" * 50)
        
        prompt = "写一个短篇故事关于时间旅行"
        print(f"原始提示：{prompt}")
        
        result = self.optimizer.optimize(prompt, TargetAI.CHATGPT, OperatingMode.DETAIL)
        print(f"\n优化后：\n{result.optimized_prompt}")
        
        if result.clarifying_questions:
            print("\n澄清问题：")
            for i, q in enumerate(result.clarifying_questions, 1):
                print(f"  {i}. {q}")
    
    def business_analysis_case(self):
        """商业分析案例"""
        print("\n📊 使用案例：商业分析")
        print("-" * 50)
        
        prompt = "分析我们的市场策略"
        print(f"原始提示：{prompt}")
        
        result = self.optimizer.optimize(prompt, TargetAI.CLAUDE, OperatingMode.DETAIL)
        print(f"\n优化后：\n{result.optimized_prompt}")
        
        if result.pro_tip:
            print(f"\n💡 专业提示：{result.pro_tip}")
    
    def comparison_example(self):
        """对比示例：同一提示在不同模式下的优化"""
        print("\n🔄 对比示例：BASIC vs DETAIL 模式")
        print("-" * 50)
        
        prompt = "创建一个移动应用的用户界面设计"
        
        # BASIC 模式
        print(f"原始提示：{prompt}")
        print("\n【BASIC 模式】")
        basic_result = self.optimizer.optimize(prompt, TargetAI.CHATGPT, OperatingMode.BASIC)
        print(self.optimizer.format_response(basic_result))
        
        print("\n" + "="*50 + "\n")
        
        # DETAIL 模式
        print("【DETAIL 模式】")
        detail_result = self.optimizer.optimize(prompt, TargetAI.CHATGPT, OperatingMode.DETAIL)
        print(self.optimizer.format_response(detail_result))


def main():
    """运行所有案例"""
    examples = UseCaseExamples()
    
    print("🚀 Lyra 优化器 - 实际使用案例")
    print("=" * 80)
    
    # 运行各种案例
    examples.marketing_email_case()
    print("\n" + "="*80)
    
    examples.code_generation_case()
    print("\n" + "="*80)
    
    examples.educational_content_case()
    print("\n" + "="*80)
    
    examples.creative_writing_case()
    print("\n" + "="*80)
    
    examples.business_analysis_case()
    print("\n" + "="*80)
    
    examples.comparison_example()
    
    print("\n\n✅ 所有案例演示完成！")


if __name__ == "__main__":
    main()