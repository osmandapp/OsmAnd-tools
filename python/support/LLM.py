import os
import re
import time

from python.lib.OpenAIClient import OpenAIClient

MODEL_FULL_NAME = os.getenv('MODEL_FULL_NAME', "")
MODEL_NAME = os.getenv('MODEL_NAME', "")

if not MODEL_FULL_NAME:
    raise ValueError("MODEL_FULL_NAME is not set to run LLM")


# An Ollama client that handles LLM calls both in “normal” mode and using external knowledge.
class LLM:
    def __init__(self, prompts):
        self.prompts = prompts

        api_key = os.getenv('LLM_API_KEY', "ollama")
        self.client = OpenAIClient(MODEL_FULL_NAME, api_key)

    @staticmethod
    def _extract_tag(tag: str, xml_string: str) -> str:
        # Create a regex pattern to match the tag and its content
        pattern = rf"<{tag}>(.*?)</{tag}>"
        match = re.search(pattern, xml_string, re.DOTALL)
        if match:
            return match.group(1)

        return ''

    @staticmethod
    def _error_ticket(error: str, description: str) -> dict:
        e = {
            "summary": error,
            "purchase": "",
            "category": "Error",
            "answer": description,
            "sentiment": "",
            "action": ""
        }
        print(f"Error {error} : {description}")
        return e

    def _extract_ticket(self, prompt: str, text: str) -> dict:
        match = re.search(r'<ticket>.*?</ticket>', text, re.DOTALL)
        if not match:
            print(f"Prompt: {prompt}")
            return self._error_ticket("LLM incorrect response", f"No ticket tag found in the LLM output: {text}.")

        ticket_xml = match.group(0)
        purchase = self._extract_tag("purchase", ticket_xml)
        category = self._extract_tag("category", ticket_xml)
        sentiment = self._extract_tag("sentiment", ticket_xml)
        action = self._extract_tag("action", ticket_xml)
        try:
            rating = float(self._extract_tag("rating", ticket_xml))
        except ValueError:
            rating = None

        ticket_data = {
            "summary": self._extract_tag("summary", ticket_xml),
            "purchase": purchase.strip("'") if purchase else None,
            "category": category.strip("'") if category else None,
            "answer": self._extract_tag("answer", ticket_xml),
            "sentiment": sentiment.strip("'") if sentiment else None,
            "rating": rating,
            "action": action.strip("'") if action else None,
        }
        return ticket_data

    def ask(self, prompt: str) -> dict:
        start_time = time.time()
        answer = None
        response = ''
        try:
            response = self.client.ask('', prompt)
            answer = self._extract_ticket(prompt, response)
            return answer
        except Exception as e:
            print(f"Prompt: {prompt}")
            return self._error_ticket("LLM request error", f"{e}")
        finally:
            print(
                f"LLM response time: {time.time() - start_time}, "
                f"Input length: ({len(prompt)} bytes = {self.client.prompt_tokens} tokens), "
                f"Output length: ({len(response) if response else -1} bytes = {self.client.completion_tokens} tokens), Response: {answer}",
                flush=True)
