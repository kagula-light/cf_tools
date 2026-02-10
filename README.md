# 5G CSV 桌面分析器（Electron + React + TypeScript）

用于本地选择 CSV 文件，校验关键列并按“时间(日)+网络”汇总，输出同目录、同编码的新文件 `-统计.csv`。

## 功能说明

- 弹窗选择 CSV 文件
- 预读前 100 行并识别编码/分隔符
- 严格校验 14 个关键列是否齐全
- 按 `YYYY-MM-DD + 网络` 聚合
- 输出固定 12 列统计结果
- 输出文件重名时中止并提示
- 内置 GitHub Releases 自动更新（检查/下载/安装）

## 技术栈

- Electron + React + TypeScript
- electron-vite
- csv-parse / csv-stringify
- jschardet + iconv-lite
- dayjs
- electron-updater
- vitest

## 开发与运行

```bash
npm install
npm run dev
```

> 注意（Windows）：如果系统设置过 `ELECTRON_RUN_AS_NODE=1`，会导致 Electron 以 Node 模式启动。本项目的 `dev/preview` 脚本已自动清理该变量。

## 测试与构建

```bash
npm test
npm run build
```

Windows 打包：

```bash
npm run dist:win
```

## 自动更新配置（GitHub Releases）

`package.json` 已配置 `electron-builder.publish.provider=github`。

发布时需要环境变量：

- `GH_OWNER`：GitHub 用户或组织
- `GH_REPO`：仓库名
- `GH_TOKEN`：具备 Releases 权限的 token

示例（PowerShell）：

```powershell
$env:GH_OWNER="your-org"
$env:GH_REPO="your-repo"
$env:GH_TOKEN="ghp_xxx"
npm run dist:win
```

### GitHub Actions 自动发布（推荐）

项目已包含工作流：`.github/workflows/release.yml`

触发方式：推送语义化 tag（如 `v1.0.1`）后自动打包并发布到 GitHub Releases。

你需要在 GitHub 仓库里配置：

- `Settings -> Secrets and variables -> Actions -> Secrets`
  - `GH_TOKEN`：可发布 Release 的 PAT（建议 classic token 勾选 `repo`）
- `Settings -> Secrets and variables -> Actions -> Variables`
  - `GH_OWNER`：仓库 owner（个人或组织）
  - `GH_REPO`：仓库名

本地发布流程示例：

```bash
git tag v1.0.1
git push origin v1.0.1
```

发布成功后，Release 里应包含：安装包（`.exe`）和更新元数据（如 `latest.yml`），客户端即可检测更新。

### 客户端如何“响应更新”

- 开发环境（`npm run dev`）默认不检查自动更新（避免干扰）
- 安装包运行时：
  - 启动后自动检查一次
  - 可手动点击“检查更新 / 下载更新 / 重启并安装”

如果你改了更新源（owner/repo），请确保安装包中的构建配置与发布仓库一致。

## 关键输入列（14列）

- 时间
- CI
- 网络
- 小区名称
- 5G总流量(GB)
- 5G最大用户数
- 5G上行PRB利用率(%)
- 5G下行PRB利用率(%)
- 5G上行体验速率(Mbps)
- 5G下行体验速率(Mbps)
- 5G无线接通率(%)
- 5G无线掉线率(%)
- 5G切换成功率(%)
- 5G上行平均干扰(dBm)

## 输出列（12列）

- 时间
- 网络
- 5G总流量(GB)
- 5G最大用户数
- 5G上行PRB利用率(%)
- 5G下行PRB利用率(%)
- 5G上行体验速率(Mbps)
- 5G下行体验速率(Mbps)
- 5G无线接通率(%)
- 5G无线掉线率(%)
- 5G切换成功率(%)
- 5G上行平均干扰(dBm)
