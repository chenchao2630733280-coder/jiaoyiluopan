# A股右侧交易罗盘 · 项目长期备忘

## 仓库与部署约定

- 远程：`git@github.com:chenchao2630733280-coder/jiaoyiluopan.git`（走 **SSH**，
  HTTPS 无凭据会 401）。`push.default=simple`，裸 `git push` / `git pull` 可用。
- **仓库保持 public，`.workbuddy/memory/*.md` 保留在仓库中**（用户 2026-11-05 明确决定，
  以后不要再提议撤掉或改 private）。
- `data/kline`、`data/*.json` 缓存、`state/`、`desktop/dist*`、`node_modules` 不入库（见 .gitignore）。
- 提交与推送**分成两条命令**：同一条 `&&` 链里 push 会静默不执行。
