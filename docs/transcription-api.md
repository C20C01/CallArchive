# 转写 API

独立转写使用 Bearer 令牌。默认支持 WAV、MP3、FLAC，单文件上限 25 MiB。转写任务与归档流程共用转写服务，独立提交的任务由 API 调用账号持有。

| 操作 | 接口 |
| --- | --- |
| 提交文件 | `POST /v1/transcription/jobs` |
| 查看状态和可用进度 | `GET /v1/transcription/jobs/{job_id}` |
| 读取文本结果 | `GET /v1/transcription/jobs/{job_id}/result` |
| 读取原录音 | `GET /v1/transcription/jobs/{job_id}/audio` |

提交时用 multipart 字段 `audio` 上传文件，`metadata` 填 JSON 字符串，至少包含 `request_id`。以本地服务为例：

```bash
curl --fail-with-body http://127.0.0.1:8765/v1/transcription/jobs \
  -H "Authorization: Bearer $token" \
  -F 'audio=@sample.wav' \
  -F 'metadata={"request_id":"demo-01"}'
```

提交成功返回 `202` 和 `job_id`。用该 ID 查询，状态为 `succeeded` 后可读取包含 `text` 的结果。进度取决于使用的引擎。需要热词或说话人数时，通过 `metadata.options` 传入 `hotwords`、`speaker_count`。

提交超时后可用相同请求 ID、音频和选项重试。保留期内，重复请求返回原任务 ID，内容有冲突时返回 `409`。明确失败后，重新转写使用新请求 ID。独立转写的文本和失败信息默认保留 24 小时，转写成功的原录音由清理任务删除。

[本地启动方式](../README.md#快速试用独立转写)
