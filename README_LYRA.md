# Lyra - AI Prompt Optimization Specialist

Transform any user input into precision-crafted prompts that unlock AI's full potential across all platforms using the revolutionary **4-D Methodology**.

## 🚀 Features

- **4-D Methodology**: Deconstruct → Diagnose → Develop → Deliver
- **Multi-Platform Support**: Optimized for ChatGPT, Claude, Gemini, and other AI platforms
- **Smart Mode Detection**: Automatic complexity assessment with BASIC/DETAIL modes
- **Advanced Techniques**: Chain-of-thought, few-shot learning, constraint optimization, and more
- **Interactive Interface**: Easy-to-use command-line interface

## 🛠️ Installation & Usage

### Quick Start

```bash
# Run Lyra interactively
python3 lyra_prompt_optimizer.py

# Run tests and examples
python3 test_lyra.py
```

### Usage Examples

```bash
# Simple optimization
"BASIC using ChatGPT — Write me a marketing email"

# Detailed optimization with clarifying questions
"DETAIL using Claude — Help with my resume"

# Auto-detection (defaults to appropriate mode)
"help me create a business plan"
```

## 📋 The 4-D Methodology

### 1. **DECONSTRUCT**
- Extract core intent, key entities, and context
- Identify output requirements and constraints
- Map what's provided vs. what's missing

### 2. **DIAGNOSE**
- Audit for clarity gaps and ambiguity
- Check specificity and completeness
- Assess structure and complexity needs

### 3. **DEVELOP**
- Select optimal techniques based on request type:
  - **Creative** → Multi-perspective + tone emphasis
  - **Technical** → Constraint-based + precision focus
  - **Educational** → Few-shot examples + clear structure
  - **Complex** → Chain-of-thought + systematic frameworks
- Assign appropriate AI role/expertise
- Enhance context and implement logical structure

### 4. **DELIVER**
- Construct optimized prompt
- Format based on complexity
- Provide implementation guidance

## 🎯 Optimization Techniques

### Foundation Techniques
- **Role Assignment**: Assign specific AI expertise
- **Context Layering**: Add structured background information
- **Output Specifications**: Define clear requirements
- **Task Decomposition**: Break complex tasks into steps

### Advanced Techniques
- **Chain-of-Thought**: Add reasoning frameworks
- **Few-Shot Learning**: Provide relevant examples
- **Multi-Perspective Analysis**: Multiple viewpoint consideration
- **Constraint Optimization**: Add specific parameters and limits

## 🤖 Platform-Specific Optimizations

### ChatGPT/GPT-4
- Structured sections with clear headers
- Conversation starters
- System message optimization
- **Best for**: Dialogue, creative writing, general tasks

### Claude
- Longer context utilization
- Reasoning frameworks
- Detailed analytical structures
- **Best for**: Analysis, reasoning, complex tasks

### Gemini
- Creative task enhancement
- Comparative analysis structures
- Multimodal considerations
- **Best for**: Creativity, comparison, visual tasks

### Other Platforms
- Universal best practices
- Platform-agnostic optimization
- **Best for**: General-purpose applications

## 📊 Example Transformations

### Before (Vague)
```
"help with my resume"
```

### After (Optimized)
```
You are a career development expert specializing in resume optimization.

Context: Add relevant background information about your industry, 
experience level, and target positions.

Task: help with my resume

Please provide a detailed and specific response.

Output Requirements:
- Clear and well-organized response
- Comprehensive coverage of the topic
- Professional tone

Constraints:
- Keep response concise and focused
```

### Before (Complex)
```
"DETAIL using Claude — Analyze AI impact on jobs"
```

### After (Optimized)
```
You are a technical specialist with expertise in AI and labor economics.

Task: Analyze the impact of AI on job markets, considering economic, 
social, and technological factors

Please provide a detailed and specific response.

Output Requirements:
- Precise and accurate information
- Step-by-step approach when applicable
- Technical clarity

Constraints:
- Provide comprehensive and detailed analysis
- Consider multiple perspectives
- Include specific examples and data where relevant
```

## 🔧 API Usage

```python
from lyra_prompt_optimizer import LyraPromptOptimizer

# Initialize Lyra
lyra = LyraPromptOptimizer()

# Optimize a prompt
result = lyra.optimize_prompt("BASIC using ChatGPT — Write a blog post about AI")

print(result)
```

## 📈 Response Formats

### Simple Requests (BASIC Mode)
```
**Your Optimized Prompt:**
[Improved prompt]

**What Changed:** [Key improvements]
```

### Complex Requests (DETAIL Mode)
```
**Your Optimized Prompt:**
[Improved prompt]

**Key Improvements:**
• [Primary changes and benefits]

**Techniques Applied:** [Brief mention]

**Pro Tip:** [Usage guidance]
```

## 🧪 Testing

Run the comprehensive test suite:

```bash
python3 test_lyra.py
```

Tests include:
- Basic and advanced optimization scenarios
- 4-D methodology component validation
- Platform-specific optimization differences
- Error handling and edge cases

## 🎯 Best Practices

1. **Be Specific**: Include target platform and desired detail level
2. **Provide Context**: More context = better optimization
3. **Iterate**: Test optimized prompts and refine based on results
4. **Match Complexity**: Use DETAIL mode for complex, professional tasks
5. **Platform Awareness**: Leverage platform-specific strengths

## 🔍 Advanced Features

### Auto-Detection
- Automatically detects prompt complexity
- Suggests optimal mode (BASIC/DETAIL)
- Provides override options

### Smart Context Enhancement
- Identifies missing context elements
- Suggests relevant background information
- Maintains original intent while adding clarity

### Technique Selection
- AI-powered technique matching
- Request type classification
- Platform-specific adaptations

## 📝 Contributing

Lyra is designed to be extensible. Key areas for enhancement:

1. **New Optimization Techniques**: Add to `optimization_techniques` dictionary
2. **Platform Support**: Extend `platform_optimizations` configuration
3. **Request Type Detection**: Enhance `_determine_request_type()` method
4. **Output Formatting**: Customize response templates

## 🎉 Welcome Message

When you first run Lyra, you'll see:

```
Hello! I'm Lyra, your AI prompt optimizer. I transform vague requests 
into precise, effective prompts that deliver better results.

**What I need to know:**
- **Target AI:** ChatGPT, Claude, Gemini, or Other
- **Prompt Style:** DETAIL (I'll ask clarifying questions first) or BASIC (quick optimization)

**Examples:**
- "DETAIL using ChatGPT — Write me a marketing email"
- "BASIC using Claude — Help with my resume"

Just share your rough prompt and I'll handle the optimization!
```

---

**Transform your AI interactions with Lyra - where every prompt becomes a precision instrument for better results! 🚀**