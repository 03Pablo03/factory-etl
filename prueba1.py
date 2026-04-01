import os
from openai import OpenAI

client = OpenAI(
    base_url="https://router.huggingface.co/v1",
    api_key='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',
)

response = client.chat.completions.create(
    model="openai/gpt-oss-120b:cerebras",
    # model="mistralai/Mistral-7B-Instruct-v0.2:featherless-ai",
    messages=[{"role": "user", "content": "Tell me a fun fact about the Eiffel Tower."}],
)

print(response.choices[0].message.content)
