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

    def ask_with_image(self, system_prompt: str, images: List[Tuple[str, str]], init_enum: int = 1, image_prompt: str = "Score image and provide justifying reasons.") -> Tuple[str, bool]:
        start_time = time.time()
        self._init()

        content = []
        i = 0
        for i, im in enumerate(images):
            format = file_name_image_format_lowercase(im[0])
            content.append({"type": "text", "text": image_prompt})
            content.append(
                {"type": "image_url", "image_url": {"url": f"data:image/{format};base64,{im[1]}"}})

        response = self.client.chat.completions.create(model=self.model,
                                                       messages=[{"role": "system", "content": system_prompt},
                                                                 {"role": "user", "content": content}],
                                                       max_tokens=512 + 256 * len(images), n=1, temperature=MODEL_TEMPERATURE, top_p=top_p, stream=False)
        if response.usage:
            self.prompt_tokens = response.usage.prompt_tokens
            self.completion_tokens = response.usage.completion_tokens
        self.duration = time.time() - start_time

        print(f"#{current_thread().name}. LLM call {self.duration:.2f}s. {self.prompt_tokens} / {self.completion_tokens}. Images: {init_enum}-{i + 1}", flush=True)
        if not response.choices or len(response.choices) == 0:
            print(f"#{current_thread().name}. Warning: LLM response doesn't have 'choices'. Response: {response}", flush=True)
            return "No 'choices' in LLM response.", True

        if self.completion_tokens == 0 or not response.choices[0].message:
            print(f"#{current_thread().name}. Warning: LLM response is empty. Reason: {response}", flush=True)
            return response.choices[0].native_finish_reason, True

        return response.choices[0].message.content, False

    def request_with_image(self, system_prompt: str, images: List[Tuple[str, str]], image_prompts: List[str]) -> Tuple[str, bool]:
        start_time = time.time()
        self._init()

        content = []
        for i, im in enumerate(images):
            format = file_name_image_format_lowercase(im[0])
            content.append({"type": "text", "text": image_prompts[i]})
            content.append(
                {"type": "image_url", "image_url": {"url": f"data:image/{format};base64,{im[1]}"}})

        response = self.client.chat.completions.create(model=self.model,
                                                       messages=[{"role": "system", "content": system_prompt},
                                                                 {"role": "user", "content": content}],
                                                       max_tokens=2048 * len(images), n=1, temperature=MODEL_TEMPERATURE, top_p=top_p, stream=False, reasoning_effort="low")
        if response.usage:
            self.prompt_tokens = response.usage.prompt_tokens
            self.completion_tokens = response.usage.completion_tokens
        self.duration = time.time() - start_time

        # print(response.model_dump_json(indent=2))

        print(f"#{current_thread().name}. LLM call {self.duration:.2f}s. {self.prompt_tokens} / {self.completion_tokens}.", flush=True)
        if not response.choices or len(response.choices) == 0:
            print(f"#{current_thread().name}. Warning: LLM response doesn't have 'choices'. Response: {response}", flush=True)
            return "No 'choices' in LLM response.", True

        if self.completion_tokens == 0 or not response.choices[0].message:
            print(f"#{current_thread().name}. Warning: LLM response is empty. Reason: {response}", flush=True)
            return response.choices[0].native_finish_reason, True

        return response.choices[0].message.content, False

def file_name_image_format_lowercase(file_name):
    file_ext = os.path.splitext(file_name)[1].lower().lstrip('.')
    if file_ext == 'jpg':
        return 'jpeg'
    return file_ext
