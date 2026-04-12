# Usage:
#   dage plan "$(cat ~/repo/_me/dage/templates/opt-prob.md)" --skills vibe-iris vibe-cod vibe-review --run
#
# Customize: replace {{...}} placeholders before running.

显式调用vibe-iris skill分析下列文档：
@{{problem-description-file}}

从用户可见的端到端入口API到问题终止点，逐层追踪完整调用链，识别每一层的复杂度阶数和常数因子开销。对标业界/竞品同层实现，量化差距。不从组件内部入手，从端到端入口向下追踪。

对于每个识别出的优化点：
- 按改动规模分级: 低垂的果实（快赢）先行并行，需研究的先出context再出produce
- {{hard-constraints}}
- 产物放 {{output-dir}}/{point-name}/，含：
  README.md(大白话/做什么/为什么/LOC/预期收益带推导/在macos的模拟验证结果)
  patch/(git format-patch)
  verify/(最小验证环境+一键式脚本(模拟环境和真实环境))

最后：
1. 端到端收益合成，量化模型，多档估算
2. 架构反思: 当前设计为什么走到这一步，根因是什么
3. 如果只有1个月的时间，上线几个，选哪几个，为什么

## Skill路由：生成YAML时按此表为每个node指定skills字段

| node role | skills                   | rationale                                  |
|-----------|--------------------------|--------------------------------------------|
| context   | [vibe-iris]              | analysis and exploration, no code changes  |
| produce   | [vibe-iris, vibe-cod]    | analysis-informed coding, repo conventions |
| gate      | [vibe-iris, vibe-review] | analysis-informed review for correctness   |
| meta      | [vibe-iris]              | synthesis, benefit estimation, reflection  |

不要在defaults中设置skills，每个node必须显式指定。
