from __future__ import annotations

import torch
from transformers import AutoModelForCausalLM, AutoTokenizer

from ..config_schema import LLMConfig


class LLMClient:
    def __init__(self, config: LLMConfig):
        self.config = config
        self.device = config.device

        if config.load_in_8bit:
            self.tokenizer = AutoTokenizer.from_pretrained(config.model_name)
            self.model = AutoModelForCausalLM.from_pretrained(
                config.model_name,
                load_in_8bit=True,
                device_map="auto",
            )
        else:
            self.tokenizer = AutoTokenizer.from_pretrained(config.model_name)
            is_cuda = config.device.startswith("cuda")
            dtype = torch.float16 if is_cuda else torch.float32
            self.model = AutoModelForCausalLM.from_pretrained(
                config.model_name,
                torch_dtype=dtype,
                low_cpu_mem_usage=True,
            )
            if is_cuda:
                self.model = self.model.to(config.device)

        self.model.eval()

    def generate(self, prompt: str) -> str:
        inputs = self.tokenizer(
            prompt,
            return_tensors="pt",
            truncation=True,
            max_length=2048,
        )
        if self.device.startswith("cuda") and not getattr(self.model, "quantization_method", None):
            inputs = {k: v.to(self.device) for k, v in inputs.items()}

        with torch.no_grad():
            outputs = self.model.generate(
                **inputs,
                max_new_tokens=self.config.max_new_tokens,
                temperature=self.config.temperature,
                do_sample=self.config.temperature > 0,
                top_p=0.9,
                pad_token_id=self.tokenizer.eos_token_id,
            )

        input_len = inputs["input_ids"].shape[1]
        generated = self.tokenizer.decode(
            outputs[0][input_len:], skip_special_tokens=True
        )
        return generated
