# Fun-ASR-Nano 转写

项目默认使用演示引擎。使用真实的本地语音识别时，安装 Nano 依赖：

```bash
python -m pip install '.[nano]'
```

准备 [Fun-ASR-Nano INT8](https://k2-fsa.github.io/sherpa/onnx/funasr-nano/pretrained.html)、[FSMN-VAD](https://www.modelscope.cn/models/iic/speech_fsmn_vad_zh-cn-16k-common-pytorch) 和 [CAM++](https://www.modelscope.cn/models/iic/speech_campplus_sv_zh-cn_16k-common) 模型。Nano 目录保留 `encoder_adaptor.int8.onnx`、`llm.int8.onnx`、`embedding.int8.onnx` 和 `Qwen3-0.6B/`，后两个模型分别放入各自目录。

```toml
[transcription]
engine = "nano"
engine_id = "nano-int8-v1"
nano_model_dir = "data/models/nano"
nano_vad_model_dir = "data/models/vad"
nano_speaker_model_dir = "data/models/speaker"
```

转写 API 和任务进程共用引擎 ID。更换模型时分配新 ID，并保留能接续旧队列的任务进程。独立双声道可按左右分轨识别，混合或重复声道走单声道识别。结果可包含带时间的分段和说话人或声道信息。模型文件需自行取得。
