from __future__ import annotations

import asyncio
import logging
import os
import time
from dataclasses import dataclass, field

from openai import AsyncOpenAI, OpenAI

from ..config_schema import LLMConfig, LLMModelConfig

logger = logging.getLogger(__name__)


@dataclass
class TokenStats:
    input_tokens: int = 0
    output_tokens: int = 0
    n_calls: int = 0
    n_failures: int = 0
    by_stage: dict[str, dict] = field(default_factory=dict)
    errors: dict[str, list[str]] = field(default_factory=dict)
    _max_errors_per_stage: int = 10

    def add(self, stage: str, in_tok: int, out_tok: int) -> None:
        self.input_tokens += in_tok
        self.output_tokens += out_tok
        self.n_calls += 1
        s = self.by_stage.setdefault(
            stage, {"input_tokens": 0, "output_tokens": 0, "n_calls": 0, "n_failures": 0}
        )
        s["input_tokens"] += in_tok
        s["output_tokens"] += out_tok
        s["n_calls"] += 1

    def add_failure(self, stage: str, error_msg: str = "") -> None:
        self.n_failures += 1
        s = self.by_stage.setdefault(
            stage, {"input_tokens": 0, "output_tokens": 0, "n_calls": 0, "n_failures": 0}
        )
        s["n_failures"] = s.get("n_failures", 0) + 1
        if error_msg:
            errs = self.errors.setdefault(stage, [])
            if len(errs) < self._max_errors_per_stage:
                errs.append(error_msg)

    def to_dict(self) -> dict:
        return {
            "total_input_tokens": self.input_tokens,
            "total_output_tokens": self.output_tokens,
            "total_calls": self.n_calls,
            "total_failures": self.n_failures,
            "by_stage": self.by_stage,
            "errors_sample": self.errors,
        }


class LLMClient:
    def __init__(self, config: LLMConfig):
        self.config = config

        api_key = config.api_key or os.environ.get(config.api_key_env, "")
        if not api_key:
            raise ValueError(
                f"No API key provided. Set llm.api_key in config or env var "
                f"`{config.api_key_env}`."
            )

        client_kwargs: dict = {"api_key": api_key, "base_url": config.base_url}
        if config.folder:
            client_kwargs["project"] = config.folder

        self.async_client = AsyncOpenAI(**client_kwargs)
        self.sync_client = OpenAI(**client_kwargs)

        self._semaphore = asyncio.Semaphore(config.max_concurrency)
        self.stats = TokenStats()

    def _model_uri(self, model_cfg: LLMModelConfig) -> str:
        if self.config.folder and not model_cfg.model_id.startswith("gpt://"):
            return f"gpt://{self.config.folder}/{model_cfg.model_id}"
        return model_cfg.model_id

    @staticmethod
    def _extract_usage(resp) -> tuple[int, int]:
        usage = getattr(resp, "usage", None)
        if usage is None:
            return 0, 0
        in_tok = getattr(usage, "input_tokens", None)
        out_tok = getattr(usage, "output_tokens", None)
        if in_tok is None:
            in_tok = getattr(usage, "prompt_tokens", 0) or 0
        if out_tok is None:
            out_tok = getattr(usage, "completion_tokens", 0) or 0
        return int(in_tok or 0), int(out_tok or 0)

    @staticmethod
    def _extract_text(resp) -> str:
        text = getattr(resp, "output_text", None)
        if text:
            return text
        # fallback for chat-completions-style responses
        choices = getattr(resp, "choices", None)
        if choices:
            try:
                return choices[0].message.content or ""
            except AttributeError:
                pass
        return ""

    async def generate_async(
        self,
        model_cfg: LLMModelConfig,
        instructions: str,
        prompt: str,
        stage: str = "general",
    ) -> str:
        last_err: Exception | None = None
        async with self._semaphore:
            for attempt in range(self.config.max_retries):
                try:
                    resp = await self.async_client.responses.create(
                        model=self._model_uri(model_cfg),
                        temperature=model_cfg.temperature,
                        instructions=instructions,
                        input=prompt,
                        max_output_tokens=model_cfg.max_output_tokens,
                    )
                    in_tok, out_tok = self._extract_usage(resp)
                    self.stats.add(stage, in_tok, out_tok)
                    return self._extract_text(resp)
                except Exception as e:  # noqa: BLE001 — retry any transient
                    last_err = e
                    if attempt == self.config.max_retries - 1:
                        break
                    delay = self.config.retry_base_delay * (2 ** attempt)
                    logger.warning(
                        "[%s] LLM call failed (attempt %d/%d): %s; retrying in %.1fs",
                        stage, attempt + 1, self.config.max_retries, e, delay,
                    )
                    await asyncio.sleep(delay)
        err_msg = f"{type(last_err).__name__}: {str(last_err)[:200]}" if last_err else "unknown"
        self.stats.add_failure(stage, error_msg=err_msg)
        logger.error("[%s] LLM call failed after %d attempts: %s", stage, self.config.max_retries, last_err)
        return ""

    def generate(
        self,
        model_cfg: LLMModelConfig,
        instructions: str,
        prompt: str,
        stage: str = "general",
    ) -> str:
        last_err: Exception | None = None
        for attempt in range(self.config.max_retries):
            try:
                resp = self.sync_client.responses.create(
                    model=self._model_uri(model_cfg),
                    temperature=model_cfg.temperature,
                    instructions=instructions,
                    input=prompt,
                    max_output_tokens=model_cfg.max_output_tokens,
                )
                in_tok, out_tok = self._extract_usage(resp)
                self.stats.add(stage, in_tok, out_tok)
                return self._extract_text(resp)
            except Exception as e:  # noqa: BLE001
                last_err = e
                if attempt == self.config.max_retries - 1:
                    break
                delay = self.config.retry_base_delay * (2 ** attempt)
                logger.warning(
                    "[%s] LLM call failed (attempt %d/%d): %s; retrying in %.1fs",
                    stage, attempt + 1, self.config.max_retries, e, delay,
                )
                time.sleep(delay)
        err_msg = f"{type(last_err).__name__}: {str(last_err)[:200]}" if last_err else "unknown"
        self.stats.add_failure(stage, error_msg=err_msg)
        logger.error("[%s] LLM call failed after %d attempts: %s", stage, self.config.max_retries, last_err)
        return ""
