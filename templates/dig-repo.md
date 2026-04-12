# Usage:
#   dage plan "$(cat ~/repo/_me/dage/templates/dig-repo.md)" --run
#
# Customize: edit the config block below before running.

# 仓库知识挖掘: 系统性提取代码惯用法、设计模式、缺陷模式

## 目标仓库

# ---- 修改这里 ----
- 主仓名称: torch_npu
- 主仓路径: /Users/shanshan/repo/torch/torch_npu
- 分析范围: torch/torch_npu/torch_npu (空=全仓库。填子目录路径如 src/module/ 则聚焦该模块,架构调研仍扫全仓)
- 主要语言: C++
- 产物目录: /Users/shanshan/note/proj/making-skill/dig-repo-cod/pta_knowledge
# ---- 修改结束 ----

## 产物规格: 产物目录下10个markdown文件

每个文件在对应node中产出,路径相对于产物目录:

1. architecture-map.md -- 全局架构(模块结构/分层/依赖关系/构建系统/公共基础设施)
2. git-history.md -- Git历史挖掘(全量commit分类为FEATURE/BUGFIX/OPTIMIZATION/REFACTOR/INFRA/REVERT/DOC_TEST/OTHER + 高频修改文件Top30 + 各模块bug密度 + 演进方向)
3. design-patterns.md -- 设计模式(架构模式/base class与virtual/宏基础设施/enum与常量/扩展点/条件编译/按模块组织的findings)
4. coding-conventions.md -- 编码惯用法(每条规则含: 适用范围[仓库级/模块级] + 强制程度[强制/推荐/可选] + 正面描述 + good/bad代码示例 + 出处file:line及频次 + 关联反模式commit hash)
5. defect-analysis.md -- 缺陷diff全量分析(每条bugfix/revert commit: hash + root cause类别 + 涉及文件 + 缺陷描述 + 修复模式 + 可审查性[高/中/低] + 审查规则建议)
6. hotspot-analysis.md -- 缺陷热点(被bugfix频繁触及的文件 + 当前代码结构性风险 + revert专项: 被revert的原始commit引入了什么问题)
7. review-standards.md -- 审查标准(P0-P3分级。每条: 规则名 + severity + 缺陷描述 + 典型代码示例[从仓库实际代码摘取] + 审查检查方法 + 关联commit证据。类别从数据涌现不预设框架)
8. developers-casebook.md -- 开发者案例集(对全量commit做分类+深度diff: scenario->constraints->decision->alternatives->consequence->transferable experience。按开发者任务场景[而非按模块]组织决策清单)
9. validation-report.md -- 交叉验证(挑5个近期真实commit模拟code review: 用conventions+review-standards审查,记录哪些规则被用到/缺失/不可操作/互相矛盾,据此回补修正上游产物)
10. README.md -- 知识目录索引 + 一句话摘要 + 使用说明(如何被vibe-cod/vibe-review消费) + 生成元数据(日期/commit范围/文件数)

## 工作流结构与依赖

### Wave 1: Context (两个node并行)

- node survey: 全局架构调研 -> architecture-map.md
  扫描顶层目录结构(记录每个模块的目录数和文件数) -> 识别分层和模块间依赖 -> 分析构建系统(CMake/Makefile/pyproject.toml等) -> 公共基础设施(共享宏/类型/工具类) -> 数据流映射

- node git_mine: Git历史挖掘 -> git-history.md
  导出全量commit(主仓+dev仓分别导出) -> 按message分类 -> 高频修改文件统计 -> revert/bugfix/refactor commit标注 -> 各模块bug密度

### Wave 2: Deep Analysis (三个node并行,均依赖Wave 1完成)

- node code_read: 深度代码阅读 -> design-patterns.md
  穷尽性阅读,不采样不跳过。Glob列出范围内全部源文件 -> 先读接口/头文件理解设计,再读实现理解做法 -> 单轮读不完就写到哪算哪下轮继续(append模式) -> 每个模块分析: 组织方式/命名/错误处理/资源管理/测试模式/反模式 -> 用Grep做模式频率统计验证观察 -> 跨模块对比(同一模式的变体及原因)

- node defect_deep: 缺陷diff全量分析 -> defect-analysis.md
  取git-history.md中全部BUGFIX+REVERT commit -> 每条用git show读完整diff -> 分析root cause/fix pattern/可审查性 -> 全量做完不采样(大仓每轮15-20条,多轮迭代) -> 分析结果增量append到产物文件

- node hotspot: 热点与Revert分析 -> hotspot-analysis.md
  统计被bugfix commit频繁触及的文件(Top20) -> 对每个热点文件Read当前代码评估结构性风险 -> Revert专项: 找出所有revert,追溯原始commit,分析逃逸原因

### Wave 3: Synthesis (三个node并行,均依赖Wave 2完成)

- node conventions: 惯用法综合 -> coding-conventions.md
  通读architecture-map + design-patterns + git-history -> 按维度提取规则(每条可直接转化为编码动作或检查项) -> 标注范围和强度 -> good/bad示例 -> 交叉验证无矛盾

- node review_std: 审查标准综合 -> review-standards.md
  通读defect-analysis + hotspot-analysis -> 聚合缺陷模式(类别涌现) -> 每类: 频次/典型案例带hash/审查检查点 -> 按频次x严重度排序 -> 输出P0-P3分级标准

- node casebook: 开发者案例集 -> developers-casebook.md
  对全量commit做分类+深度diff读(不精选不采样,全量做完) -> 每条提炼决策点(scenario/constraints/decision/alternatives/consequence) -> 识别"错误->修复"commit对 -> 重组为场景化指南

### Wave 4: Meta (串行,依赖Wave 3完成)

- node validation: 交叉验证 -> validation-report.md
  挑5个近期commit(混合feature/bugfix/refactor) -> 用conventions+review-standards做模拟review -> 记录命中/缺失/不可操作的规则 -> 据此patch上游产物(直接编辑conventions和review-standards) -> 检查两份产物是否互相矛盾

- node package: 打包 -> README.md
  汇总全部产物 -> 生成索引和使用说明

## 关键方法论约束(必须写入对应node的prompt)

1. 穷尽性阅读: 范围内每个源文件都要读,不采样。读不完就增量写入下轮继续
2. 增量写入: 不要在context中攒发现,每完成一批就append到产物文件
3. 证据标准: 每个观察必须有file:line佐证,反模式必须有commit hash证据,无证据标注"推断未确认"
4. 类别涌现: defect分类和review-standards类别从数据涌现,不预设框架。dev仓与主仓的缺陷特征可能不同
5. dev仓处理: 如有dev仓,git命令分别在两个仓库执行。commit hash完全不重叠。可参考主仓已有模式作为背景知识但不作为预设框架
6. 频率量化: 用Grep count量化模式普遍程度(如"某宏出现N次覆盖M个文件"),区分"强制规范"(100%一致)和"常见做法"(多数遵循)

## YAML生成约束

1. defect_deep, code_read, casebook等全量迭代节点: 不设timeout, prompt必须包含批次指令("每轮处理15-20条,结果追加到产物文件,处理完全部后输出NODE_COMPLETE")
