# 配置、启动与定时任务

从 `callarchive.example.toml` 复制 `callarchive.toml`。统一将该文件传给各进程。配置中的相对路径以 TOML 所在目录为基准。

```bash
python -m callarchive.configuration --config callarchive.toml
```

## 身份与服务

归档库与转写库各自保存状态。将以下三个进程交给进程管理器托管：

| 进程 | 命令 |
| --- | --- |
| 转写 API | `python -m callarchive serve --config callarchive.toml` |
| 转写任务进程 | `python -m callarchive worker --config callarchive.toml` |
| 归档 API 和网页 | `python -m callarchive.backend serve --config callarchive.toml` |

- API 从 `CALLARCHIVE_IDENTITIES` 读取身份。值是 JSON 对象，以账号名为键，每个账号配置唯一的 `token`。归档 API 的人员令牌可用来登录网页，管理员账号列入 `backend.admins`。
- 在转写 API 配置专用身份并设 `service: true`。归档 API、日常批次和归档清理使用该账号的令牌，即 `CALLARCHIVE_TRANSCRIPTION_TOKEN`。
- 如需网页提交独立转写，在转写 API 配置另一个非服务身份，将其名称填到 `transcription.web_gateway_owner`，并向归档 API 注入其令牌 `CALLARCHIVE_BROWSER_TRANSCRIPTION_TOKEN`。
- 如需转写完成后立刻推进归档，给归档 API 和转写任务进程注入同一个 `CALLARCHIVE_ARCHIVE_TRIGGER_TOKEN`，并给转写任务进程配置 `CALLARCHIVE_ARCHIVE_TRIGGER_URL`，指向内部归档 API 根地址。定时批次也可接续未送达的回调。
- 上线时使用 HTTPS 反向代理并启用 `backend.secure_cookies`，内部接口由服务间私有网络访问。真实账号、密钥和数据库保存在发布目录之外。

默认端口是转写 API **8765**、归档网页 **8766**。进行真实文件识别前，配置[Nano](nano.md)或使用 `transcription.engine = "command"`。命令引擎的 `command` 是参数数组，任务进程顺序追加音频、输入选项 JSON 和输出结果 JSON 的路径。脚本需用 UTF-8 写出含 `text` 字段的结果。API 和任务进程应使用相同的引擎 ID 和转写库。

## 定时任务

以服务账号运行下列命令，均加 `--config /绝对路径/callarchive.toml`。时间以业务时区为准，配置 cron 时核实主机实际采用的时区。

| 周期 | 命令 |
| --- | --- |
| 08:00 至 21:55 每 5 分钟，夜间每小时 | `python -m callarchive.backend run-due` |
| 默认每天 09:43 | `python -m callarchive.backend.report --scheduled` |
| 每小时 | `python -m callarchive.backend cleanup` 和 `python -m callarchive cleanup` |

为同一实例的日常批次加进程锁，避免重复启动。日报单独触发。归档和转写数据分别保存在两份 SQLite 库，备份时应在同一停写窗口保存两库和附件目录。更换服务版本时保留在途任务绑定的转写服务身份、引擎 ID 和适配器实例名。

[配置仟客、北森与飞书](integrations.md)
