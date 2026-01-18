from __future__ import annotations

import os
from typing import Iterable, Tuple

import requests


OPENAI_URL = "https://api.openai.com/v1/chat/completions"


def _format_top_states(states: Iterable[Tuple[str, int]]) -> str:
    items = [f"{state} ({count})" for state, count in states]
    return ", ".join(items) if items else "None"


def summarize_bump_entry(
    decade_label: str,
    disaster_type: str,
    top_states: Iterable[Tuple[str, int]],
    timeout_s: int = 20,
) -> str:
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        raise RuntimeError("OPENAI_API_KEY is not set.")

    model = os.getenv("OPENAI_MODEL", "gpt-4o-mini")
    top_states_text = _format_top_states(top_states)

    system_prompt = (
        "You are a concise disaster analyst. Provide a broad thematic overview of the "
        "selected disaster type and decade, highlighting key themes or notable events. "
        "Then summarize the most affected states in bullets, naming notable instances "
        "of the disaster type and impacts where possible. Use cautious language and do not "
        "invent facts."
    )
    user_prompt = (
        f"Disaster type: {disaster_type}\n"
        f"Decade: {decade_label}\n"
        f"Top affected states (by count): {top_states_text}\n"
        "Format:\n"
        "1) A short paragraph (2-4 sentences) with broad thematic overview and key themes.\n"
        "2) A bulleted list of the most affected states, each bullet naming notable instances "
        "and impacts where known. If unsure, say so explicitly.\n"
    )

    payload = {
        "model": model,
        "temperature": 0.3,
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
    }
    headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}
    resp = requests.post(OPENAI_URL, json=payload, headers=headers, timeout=timeout_s)
    resp.raise_for_status()
    data = resp.json()
    return data["choices"][0]["message"]["content"].strip()
