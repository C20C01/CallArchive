"""Select installed adapters at the application's composition boundary."""

import os
from importlib import import_module

import httpx

from ..archive.http import HTTPArchive


def driver_factory(specification, builtins=None):
    specification = (builtins or {}).get(specification, specification)
    module, separator, name = specification.partition(":")
    if not separator or not name.isidentifier() or not all(part.isidentifier() for part in module.split(".")):
        raise ValueError("Adapter driver must identify an installed module and factory.")
    try:
        factory = getattr(import_module(module), name)
    except (ImportError, AttributeError):
        raise ValueError("Unable to load the configured adapter driver.") from None
    if not callable(factory):
        raise ValueError("The configured adapter factory must be callable.")
    return factory


def provider_headers(token_env):
    token = os.getenv(token_env) if token_env else None
    if token_env and not token:
        raise ValueError("Set the configured provider credential.")
    return {"Authorization": f"Bearer {token}"} if token else {}


async def configured_archive(resources, name, settings):
    if settings.driver != "http":
        factory = driver_factory(settings.driver, {"beisen": "callarchive.archive.beisen:create_archive"})
        return factory(resources, name, settings.options)
    client = await resources.enter_async_context(httpx.AsyncClient(
        base_url=str(settings.url), headers=provider_headers(settings.token_env), timeout=20, trust_env=False,
    ))
    return HTTPArchive(name, client, supports_idempotency=settings.supports_idempotency,
                       supports_verification=settings.supports_verification)
