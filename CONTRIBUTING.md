# 贡献与发布流程

Develop on short-lived `codex/...` branches, open a pull request against `main`,
and merge only after all required CI checks pass. Release a tested commit using
a version tag; merging a PR does not publish a package.

## 分支约定

- `main`：经过检查的集成基线，不直接推送开发改动。
- `codex/<任务名>`：一个独立任务一个短期分支，修复、功能和文档调整都走 PR。
- `v<版本号>`：正式版本标签，指向通过 CI 的 `main` 提交。

当前采用 `main` 加短期任务分支，不设长期 `develop`。等到需要同时维护多个正式版本时，再按需建立 `release/<版本系列>` 分支。

## 开发与检查

在工作区干净时，从最新 `main` 建立分支：

```powershell
git switch main
git pull --ff-only origin main
git switch -c codex/your-task
conda activate soul-plugin-core
```

使用 Conda 环境 `soul-plugin-core`，不创建项目 `.venv`。首次安装开发依赖可执行 `python -m pip install -e ".[dev]"`。提交前运行：

```powershell
python -m pytest -q
python -m mypy
python -m ruff check .
python -m ruff format --check .
python -m build --no-isolation
git diff --check
```

提交本次任务相关文件，推送当前分支，然后在 GitHub 创建目标为 `main` 的 PR。尚未完成的工作可以使用 Draft PR；Draft 也运行 CI。

```powershell
git push -u origin HEAD
```

PR 说明写清解决的问题、行为变化、验证结果和已知限制。开发过程记录继续放在项目约定的交流目录；不为追踪过程把个人进度日志加入仓库。

## 合并门槛

`main` 的 GitHub 分支保护要求：

- 通过 PR 合并，管理员同样受约束，禁止强制推送和删除。
- 合并前更新到最新 `main`，解决所有 PR 讨论。
- 以下四项 GitHub Actions 检查全部通过：

| 系统 | Python | 必需检查名称 |
| --- | --- | --- |
| Ubuntu | 3.10 | `test (ubuntu-latest, 3.10)` |
| Ubuntu | 3.11 | `test (ubuntu-latest, 3.11)` |
| Windows | 3.10 | `test (windows-latest, 3.10)` |
| Windows | 3.11 | `test (windows-latest, 3.11)` |

每一项都构建 sdist 和 wheel，安装 wheel，检查依赖，运行 sdist 中的测试与示例，并执行 mypy 和 Ruff。检查名称与保护规则必须同步维护，避免改名后 PR 永久等待不存在的检查。

个人维护阶段不强制另一位审核人批准，必需批准数为 0；PR 和 CI 门槛仍然生效。团队有稳定审核人后，可以提高批准数。SonarCloud 是辅助结果，目前不列为必需检查。

如果 PR 落后于 `main`，在任务分支合并最新基线，推送后重新等待 CI：

```powershell
git fetch origin
git merge origin/main
git push
```

CI 在目标为 `main` 的 PR、`main` 的提交和 `v*` 标签上运行；尚未创建 PR 的分支可在 Actions 页面手动运行。开发分支普通 push 不重复触发另一套检查，同一 PR 新提交会取消过期运行。不使用路径过滤跳过必需检查。

检查通过后优先使用 Squash and merge。合并后切回 `main` 并 `git pull --ff-only origin main`；下一项任务重新建分支。已完成的远端任务分支可以在 PR 页面删除。

## 版本发布

合并到 `main` 和发布是两个独立动作。当前没有自动发布到 PyPI 的流程，合并 PR 或推送标签都不会上传包。

1. 用版本准备 PR 同步更新 `pyproject.toml` 和 `src/soul_plugin_core/__init__.py` 中的版本，记录兼容性变化并通过上述检查。
2. 合并后确认 `main` 对应提交的 CI 已成功，确定发布所用的准确提交 SHA。
3. 给该提交建立带说明的 `v<版本号>` 标签，再推送该单个标签。不要移动已发布标签；修复使用新版本。
4. 等标签 CI 成功后，在 GitHub 创建对应 Release，说明变化与限制。若附加构建产物，应从该标签的干净检出构建，不能复用混有旧版本的 `dist/`。
5. PyPI 上传属于另一个明确的发布动作；需要发布凭据和单独配置，目前不自动执行。

标签是版本锚点，`main` 可以在发布后继续接收通过检查的改动。正式版本消费者固定版本或标签，开发联调才跟随任务分支或指定提交。

分支保护是 GitHub 仓库设置，不会随克隆复制；迁移仓库时需按上述清单重新配置。设置依据见 [GitHub 分支保护文档](https://docs.github.com/en/repositories/configuring-branches-and-merges-in-your-repository/managing-protected-branches/about-protected-branches)。
