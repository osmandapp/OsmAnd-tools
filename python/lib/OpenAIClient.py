import logging
import os
import time
from threading import current_thread
from typing import List, Tuple

import httpx
import openai

logging.getLogger("httpx").setLevel(logging.WARNING)

base_urls = {"ollama": "http://localhost:11434/v1", "or": "https://openrouter.ai/api/v1",
             "dp": "https://api.deepseek.com/v1", "veles": "https://veles.osmand.net:8081/api"}
MODEL_TEMPERATURE = float(os.getenv('MODEL_TEMPERATURE', 0.1))
top_p = float(os.getenv('MODEL_TOP_P', 1.0))  # Controls randomness; lower is more deterministic
MAX_TOKENS = int(os.getenv('MAX_TOKENS', 8 * 1024))  # Limit the response length
LLM_TIMEOUT = float(os.getenv('LLM_TIMEOUT', 120))


class ImageCompletionError(RuntimeError):
    def __init__(self, message: str, block_reason: str | None = None):
        super().__init__(message)
        self.block_reason = block_reason


def _image_completion_error(error: dict, response_id: str | None = None) -> ImageCompletionError:
    message = str(error.get("message") or "LLM response error")
    metadata = error.get("metadata")
    if not isinstance(metadata, dict):
        metadata = {}
    provider_code = metadata.get("provider_code") or metadata.get("provider_error_code")
    prefix, _, message_reason = message.partition(":")
    reason = provider_code or message_reason.strip()
    block_reason = reason if (
        prefix in ("Gemini blocked the request", "Gemini blocked the response")
        and reason in ("PROHIBITED_CONTENT", "OTHER")
    ) else None
    details = {
        "code": error.get("code"),
        "error_type": metadata.get("error_type"),
        "provider_code": provider_code,
        "response_id": response_id,
    }
    detail = ", ".join(f"{key}={value}" for key, value in details.items() if value is not None)
    return ImageCompletionError(f"{message} ({detail})" if detail else message, block_reason)


class OpenAIClient:
    prompt_tokens = 0
    completion_tokens = 0
    duration = 0
    provider = None

    def __init__(self, model: str, api_key: str, api_url: str = None):
        parts = model.split('@')
        self.model = parts[1]
        self.provider = parts[0]
        if parts[0] not in base_urls:
            raise Exception(f"Invalid OpenAI model: {model}")
        elif parts[0] == 'ollama':
            api_key = 'ollama'

        base_url = api_url if api_url else base_urls[parts[0]]
        # Use explicit per-phase timeouts and disable HTTP/2 to avoid intermittent large-response truncation on some stacks.
        self.client = openai.OpenAI(base_url=base_url, api_key=api_key,
                                    timeout=httpx.Timeout(LLM_TIMEOUT, connect=3.0), max_retries=1)
        self._init()

    def _init(self):
        self.prompt_tokens = 0
        self.completion_tokens = 0
        self.duration = 0

    def ask(self, system_prompt: str, user_query: str, max_tokens: int = MAX_TOKENS, temperature: float = -1.0):
        if temperature < 0.0:
            temperature = MODEL_TEMPERATURE
        start_time = time.time()
        self._init()

        try:
            response = self.client.chat.completions.create(model=self.model, messages=[
                # There are the following roles: system/user/assistant
                # system - instructions (tools) for shaping behavior which is most authoritative.
                # user - input query which is medium authoritative.
                # assistant - to send back “assistant” messages (and “user” messages) as further context to more accurate completions.
                {"role": "system", "content": system_prompt}, {"role": "user", "content": user_query}],
                                                           max_tokens=max_tokens,
                                                           n=1,  # Number of completions to generate
                                                           temperature=temperature,
                                                           top_p=top_p, stream=False)
            if response.usage:
                self.prompt_tokens = response.usage.prompt_tokens
                self.completion_tokens = response.usage.completion_tokens
            return response.choices[0].message.content
        finally:
            self.duration = time.time() - start_time
            print(f"#{current_thread().name}. LLM call {self.duration:.2f}s. {self.prompt_tokens} / {self.completion_tokens}.", flush=True)

    def ask_with_image(self, system_prompt: str, images: List[Tuple[str, str]], init_enum: int = 1, image_prompt: str = "Score image and provide justifying reasons.") -> str:
        start_time = time.time()
        self._init()

        content = []
        i = 0
        for i, im in enumerate(images):
            format = file_name_image_format_lowercase(im[0])
            content.append({"type": "text", "text": image_prompt})
            content.append(
                {"type": "image_url", "image_url": {"url": f"data:image/{format};base64,{im[1]}"}})

        try:
            response = self.client.chat.completions.create(model=self.model,
                                                           messages=[{"role": "system", "content": system_prompt},
                                                                     {"role": "user", "content": content}],
                                                           max_tokens=512 + 256 * len(images), n=1, temperature=MODEL_TEMPERATURE, top_p=top_p, stream=False)
            if response.usage:
                self.prompt_tokens = response.usage.prompt_tokens
                self.completion_tokens = response.usage.completion_tokens
        except openai.APIError as error:
            body = error.body
            if isinstance(body, dict):
                provider_error = body.get("error", body)
                if isinstance(provider_error, dict):
                    completion_error = _image_completion_error(provider_error, body.get("id"))
                    if completion_error.block_reason:
                        raise completion_error from error
            raise
        finally:
            self.duration = time.time() - start_time

        print(f"#{current_thread().name}. LLM call {self.duration:.2f}s. {self.prompt_tokens} / {self.completion_tokens}. Images: {init_enum}-{i + 1}", flush=True)
        error = getattr(response, "error", None)
        if isinstance(error, dict):
            raise _image_completion_error(error, response.id)
        if not response.choices:
            raise ImageCompletionError(f"LLM response is missing choices (response_id={response.id})")

        choice = response.choices[0]
        native_reason = getattr(choice, "native_finish_reason", None)
        if native_reason in ("PROHIBITED_CONTENT", "OTHER"):
            raise ImageCompletionError(f"{native_reason} (response_id={response.id})", native_reason)
        if not choice.message or not choice.message.content:
            reason = native_reason or choice.finish_reason
            raise ImageCompletionError(f"LLM response is empty (finish_reason={reason}, response_id={response.id})")
        return choice.message.content


def file_name_image_format_lowercase(file_name):
    file_ext = os.path.splitext(file_name)[1].lower().lstrip('.')
    if file_ext == 'jpg':
        return 'jpeg'
    return file_ext
