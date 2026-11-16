# Issue tracker: GitHub

本仓库的任务和规格保存在 `LaoBan-ywcm/b-a` 的 GitHub Issues。
使用 `gh` CLI 操作。

## 操作约定

- 首次创建 issue 前，使用 `gh repo view --json nameWithOwner`
  核对目标为 `LaoBan-ywcm/b-a`。
- 创建：`gh issue create --title "..." --body-file <path>`。
  多行正文写入临时文件，保留真实换行。
- 读取：`gh issue view <number> --json number,title,body,labels,comments`。
- 列表：`gh issue list --state open --json number,title,body,labels`。
  按需添加标签和状态过滤。
- 评论：`gh issue comment <number> --body-file <path>`。
- 标签：`gh issue edit <number> --add-label "..."` 或 `--remove-label "..."`。
- 关闭：先发表评论说明原因，再执行 `gh issue close <number>`。
- 子任务优先使用 GitHub 原生 sub-issue 关系；不可用时，
  在子任务正文顶部写 `Part of #<parent>`，并在父任务中维护任务列表。
- 阻塞关系优先使用 GitHub 原生 issue dependencies；不可用时，
  在正文顶部写 `Blocked by: #<number>`。全部阻塞任务关闭后才可开始。
- 使用当前 `gh` 版本支持的命令；数据库 ID 与 issue 编号不可混用。

技能要求“publish to the issue tracker”时，表示创建 GitHub issue；
要求“fetch the relevant ticket”时，表示读取对应 issue。
这些约定不替代用户对公开发布等操作的授权要求。

## Pull requests as a triage surface

**PRs as a request surface: no.**

## Wayfinding

- 工作地图使用一个带 `wayfinder:map` 标签的 issue，
  正文维护 Notes、Decisions-so-far、Fog。
- 子任务关联地图，使用 `wayfinder:<type>` 标签，
  type 为 research、prototype、grilling 或 task。
- 按地图顺序选择未关闭、没有未关闭阻塞项且尚未分配的任务。
- 领取任务时分配给当前开发者。
- 完成时评论结果、关闭任务，并将结论摘要和链接补入地图。
