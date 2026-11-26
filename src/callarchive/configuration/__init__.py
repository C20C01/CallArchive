"""Shared configuration for the independently runnable modules."""

from datetime import time
from pathlib import Path
from typing import Annotated, Any, Literal

from pydantic import BaseModel, ConfigDict, Field, HttpUrl, ValidationError, field_validator, model_validator

try:
    import tomllib
except ModuleNotFoundError:  # Python 3.10
    import tomli as tomllib


class ConfigError(ValueError):
    pass


class Section(BaseModel):
    model_config = ConfigDict(extra="forbid")


class BackendSettings(Section):
    database: Path = Path("data/archive.sqlite")
    port: int = Field(default=8766, ge=1, le=65535)
    admins: list[str] = Field(default_factory=list)
    account_file: Path | None = None
    secure_cookies: bool = False
    scheduler_enabled: bool = False
    transcription_url: str = "http://127.0.0.1:8765"
    transcription_api_owners: dict[str, list[str]] = Field(default_factory=dict)
    transcription_admin_results: bool = False
    transcription_admin_system_results: bool = False
    followup_names: dict[str, str] = Field(default_factory=dict)

    @field_validator("transcription_api_owners")
    @classmethod
    def validate_transcription_api_owners(cls, bindings):
        owners = [owner for items in bindings.values() for owner in items]
        if any(not name.strip() for name in [*bindings, *owners]) or len(set(owners)) != len(owners):
            raise ValueError("Each transcription API identity must map to a single web account.")
        return bindings


class TranscriptionSettings(Section):
    database: Path = Path("data/transcription.sqlite")
    port: int = Field(default=8765, ge=1, le=65535)
    retention: int = Field(default=86400, gt=0)
    max_unclaimed: int = Field(default=604800, gt=0)
    web_gateway_owner: str | None = Field(default=None, min_length=1)
    engine: Literal["demo", "command", "nano"] = "demo"
    engine_id: str | None = None
    command: list[str] | None = Field(default=None, min_length=1)
    nano_model_dir: Path | None = None
    nano_vad_model_dir: Path | None = None
    nano_speaker_model_dir: Path | None = None
    nano_hotwords: list[str] = Field(default_factory=list)
    nano_opening_hotwords: list[str] = Field(default_factory=list)
    nano_opening_seconds: int = Field(default=0, ge=0)
    nano_expected_speakers: int | None = Field(default=None, ge=1, le=100)
    nano_short_review: bool = True


InstanceName = Annotated[str, Field(pattern=r"^[A-Za-z0-9_.-]{1,64}$")]


class AdapterSettings(Section):
    driver: str = Field(default="http", min_length=1)
    url: HttpUrl | None = None
    token_env: str | None = Field(default=None, pattern=r"^[A-Z_][A-Z0-9_]*$")
    options: dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="after")
    def check_driver_settings(self):
        if self.driver == "http" and (self.url is None or self.options):
            raise ValueError("HTTP adapters require a URL and use the standard connection fields.")
        if self.driver != "http" and (self.url is not None or self.token_env is not None):
            raise ValueError("Installed adapters read their connection fields from options.")
        return self


class OutboundSettings(Section):
    active_source: str | None = None
    sources: dict[InstanceName, AdapterSettings] = Field(default_factory=dict)
    manifest: Path | None = None
    url: HttpUrl | None = None
    token_env: str | None = Field(default=None, pattern=r"^[A-Z_][A-Z0-9_]*$")
    flow: Literal["local", "remote"] = "local"
    skip_call_duration_ms: int | None = Field(default=None, ge=0)
    min_free_mib: int = Field(default=1024, ge=0)
    owner: str | None = None
    source: str | None = None
    max_pages: int | None = Field(default=None, gt=0)  # Accepted for existing configuration files.
    poll_interval_seconds: int = Field(default=300, gt=0)
    poll_warning_after_seconds: int = Field(default=4500, gt=0)
    initial_lookback_seconds: int = Field(default=86400, gt=0)
    regular_lookback_seconds: int | None = Field(default=None, ge=0)
    recording_followup_interval_seconds: int = Field(default=20 * 60, gt=0)
    recording_followup_windows_per_poll: int = Field(default=12, gt=0)

    @model_validator(mode="after")
    def check_active_source(self):
        if self.active_source is not None:
            if self.active_source not in self.sources:
                raise ValueError("Active outbound source is not configured.")
            if any(item is not None for item in (self.manifest, self.url, self.token_env, self.source)):
                raise ValueError("Select a named source or use the direct outbound settings.")
        return self


class ExternalArchiveSettings(AdapterSettings):
    supports_idempotency: bool = False
    supports_verification: bool = False

    @model_validator(mode="after")
    def check_capability_settings(self):
        if self.driver != "http" and (self.supports_idempotency or self.supports_verification):
            raise ValueError("Installed archive adapters declare their own delivery capabilities.")
        return self


class ArchiveSettings(Section):
    history_retention_days: int = Field(default=7, ge=1)
    attachments: Path = Path("data/attachments")
    providers: dict[InstanceName, ExternalArchiveSettings] = Field(
        default_factory=dict,
    )
    active_provider: str | None = None
    default_flow: Literal["local", "remote"] = "local"

    @model_validator(mode="after")
    def check_active_provider(self):
        if self.active_provider is not None and self.active_provider not in self.providers:
            raise ValueError("Active archive provider is not configured.")
        if self.default_flow == "remote" and not self.active_provider:
            raise ValueError("Remote archive requires an active provider.")
        return self


class NotificationAdapterSettings(Section):
    driver: str = Field(min_length=1)
    options: dict[str, Any] = Field(default_factory=dict)


class NotificationSettings(Section):
    channel: InstanceName = "preview"
    channels: dict[InstanceName, NotificationAdapterSettings] = Field(default_factory=dict)
    timezone: str = "Asia/Hong_Kong"
    daily_at: time = time(9, 43)
    channel_id: str | None = None
    workbench_url: HttpUrl | None = None
    owner: str | None = None
    destination: str = "operator"
    page_size: int = Field(default=20, gt=0)
    output: Path = Path("data/reports")

    @model_validator(mode="after")
    def check_channel(self):
        builtins = {"preview", "feishu"}
        if builtins.intersection(self.channels):
            raise ValueError("Built-in notification names are reserved.")
        if self.channel not in builtins and self.channel not in self.channels:
            raise ValueError("Active notification channel is not configured.")
        return self

    @field_validator("daily_at")
    @classmethod
    def local_time(cls, value):
        if value.tzinfo is not None:
            raise ValueError("Daily time must be local to notifications.timezone.")
        return value


class LoggingSettings(Section):
    level: Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"] = "INFO"
    format: Literal["text", "json"] = "text"
    database: Path | None = None
    retention_days: int = Field(default=7, gt=0)
    max_rows: int | None = Field(default=None, ge=100)  # Accepted for existing configuration files.


class Settings(Section):
    backend: BackendSettings = Field(default_factory=BackendSettings)
    transcription: TranscriptionSettings = Field(default_factory=TranscriptionSettings)
    outbound: OutboundSettings = Field(default_factory=OutboundSettings)
    archive: ArchiveSettings = Field(default_factory=ArchiveSettings)
    notifications: NotificationSettings = Field(default_factory=NotificationSettings)
    logging: LoggingSettings = Field(default_factory=LoggingSettings)

    @model_validator(mode="after")
    def check_outbound(self):
        if self.outbound.flow == "remote" and self.archive.active_provider is None:
            raise ValueError("Outbound remote flow requires an active archive provider.")
        return self


def load_config(path: Path | None = None) -> Settings:
    if path is None:
        return Settings()
    try:
        with path.open("rb") as stream:
            data = tomllib.load(stream)
        result = Settings.model_validate(data)
    except OSError as error:
        raise ConfigError(f"Unable to read configuration ({type(error).__name__}).") from None
    except (tomllib.TOMLDecodeError, UnicodeError):
        raise ConfigError("Invalid TOML configuration.") from None
    except ValidationError as error:
        locations = (
            ".".join(map(str, item["loc"])) or "configuration"
            for item in error.errors(include_input=False, include_context=False, include_url=False)
        )
        raise ConfigError("Invalid configuration fields: " + ", ".join(locations)) from None

    root = path.resolve().parent
    for name in Settings.model_fields:
        section = getattr(result, name)
        for field in type(section).model_fields:
            value = getattr(section, field)
            if isinstance(value, Path):
                setattr(section, field, (root / value).resolve())
    return result


def config_from_args(parser, args):
    try:
        return load_config(args.config)
    except ConfigError as error:
        parser.error(str(error))
