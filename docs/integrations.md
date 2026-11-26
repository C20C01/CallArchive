# 仟客、北森与飞书接入

使用同一份 TOML，合并下列配置到对应的段。凭据放在相应进程的环境变量中。独立转写不需要任何一个外部服务。

## 仟客：话单和录音

准备仟客的应用 ID、企业 ID 和应用密钥。将应用密钥设置为 `QIANKE_APP_SECRET`，启用下列实例：

```toml
[outbound]
active_source = "qianke-main"
owner = "operator"
flow = "local"

[outbound.sources.qianke-main]
driver = "qianke"

[outbound.sources.qianke-main.options]
app_id = "<应用 ID>"
company_id = "<企业 ID>"
secret_env = "QIANKE_APP_SECRET"
```

`owner` 是归档后台的归属账号。运行日常批次后，适配器导入已接通话单，同一话单的多段录音归为一组，录音尚未可用时继续补查。仟客默认重扫最近 3 小时，分页读完后才推进水位。更换外呼实例时保留原实例及凭据，未完成话单仍向原实例补查。

## 北森：应聘档案和附件

准备北森连接器的 Key 和 Secret，在归档后台及其定时批次环境中分别设置 `BEISEN_CONNECTOR_KEY` 和 `BEISEN_CONNECTOR_SECRET`：

```toml
[archive]
active_provider = "beisen-main"

[archive.providers.beisen-main]
driver = "beisen"

[archive.providers.beisen-main.options]
key_env = "BEISEN_CONNECTOR_KEY"
secret_env = "BEISEN_CONNECTOR_SECRET"
requests_per_second = 10
```

将 `outbound.flow` 改为 `"remote"`，外呼录音才会走北森，直接提交归档 API 时则通过 `metadata.flow` 选择。适配器按被叫号码查找候选人和应聘档案，录音与文本分别写入匹配目标。北森适配器支持 MP3、WAV 和 TXT 附件。找不到档案的话单可补录后复查。若写入结果不明，系统保留请求和回执供核对，避免重复上传。

## 飞书：提醒和日报

创建飞书群自定义机器人，将 Webhook 设置为 `CALLARCHIVE_FEISHU_WEBHOOK`。启用签名校验时再设置 `CALLARCHIVE_FEISHU_SECRET`。将 `owner` 设为要统计的归属账号。填写 `workbench_url` 后，卡片可带待归档页面链接。

```toml
[notifications]
channel = "feishu"
owner = "operator"
destination = "operations"
daily_at = 09:43:00
timezone = "Asia/Hong_Kong"
# workbench_url = "https://archive.example.org"
```

日报列出待跟进、需管理员介入和近 24 小时归档数量。日常批次检查单条提醒，日报由单独的定时命令触发。配置 `backend.followup_names` 后，同一人的多个主叫号码可合并展示。

要先看排版，用预览渠道生成本地 JSON：

```bash
python -m callarchive.backend.report --config callarchive.toml --day 2026-01-01 --channel preview
```

把示例日期改成需要查看的日期。预览单条提醒用 `--alerts` 代替 `--day` 和日期。
