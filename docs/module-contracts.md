# 模块契约

后台通过接口调用适配器，在数据库中记录话单、任务和回执。转写可以独立部署并通过 HTTP 复用。定时入口调用业务函数，转写完成回调也可以继续推进对应话单。

| 模块 | 入口与契约 |
| --- | --- |
| 外呼 | `OutboundSource` 提供话单分页、录音补查和录音读取。话单、录音和分页游标使用稳定标识。 [接口](../src/callarchive/outbound/__init__.py) |
| 档案 | `ExternalArchive` 提供目标查询、附件写入和回查。写入的请求 ID 对每份附件保持稳定。 [接口](../src/callarchive/archive/provider.py) |
| 转写 | `TranscriptionEngine` 接收音频和选项，返回至少包含 `text` 的结果。外部调用方可使用异步任务接口。 [引擎接口](../src/callarchive/transcription/engines.py) · [转写 API](transcription-api.md) |
| 通知 | `NotificationChannel` 接收消息并返回投递回执。渠道负责渲染和发送，后台决定发送内容及去重。 [接口](../src/callarchive/notifications/__init__.py) |

外呼和档案支持独立 HTTP 适配器或 Python 工厂。通知支持 Python 工厂。工厂通过 `module:create_adapter` 形式配置，参数为 `resources`、实例名称和 `options`。HTTP 适配器的请求格式见[外呼实现](../src/callarchive/outbound/http.py)和[档案实现](../src/callarchive/archive/http.py)。

归档写入结果不明确时，只有能证实附件未写入，或服务保证相同请求 ID 幂等时，才允许自动重发。更换实例时保留旧实例的配置，直到其未完成任务处理完毕。
