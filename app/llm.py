from __future__ import annotations

import json
import os
from typing import Iterable, Tuple, Dict, List, Optional

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


def _extract_json_mapping(text: str) -> Dict[str, str]:
    start = text.find("{")
    end = text.rfind("}")
    if start == -1 or end == -1 or end <= start:
        raise ValueError("No JSON object found in response.")
    payload = text[start : end + 1]
    return json.loads(payload)


def group_declaration_names(
    names: List[str],
    timeout_s: int = 30,
    chunk_size: int = 100,
) -> Dict[str, str]:
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        raise RuntimeError("OPENAI_API_KEY is not set.")

    model = os.getenv("OPENAI_MODEL", "gpt-4o-mini")
    cleaned = [n.strip() for n in names if n and n.strip()]
    if not cleaned:
        return {}

    system_prompt = (
        "You normalize disaster declaration names. Group related names under a single "
        "canonical label when they refer to the same named disaster. Do not invent new "
        "events or locations. If the name is generic or unclear, keep it unchanged."
    )

    headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}
    mapping: Dict[str, str] = {}
    for i in range(0, len(cleaned), chunk_size):
        chunk = cleaned[i : i + chunk_size]
        user_prompt = (
            "Return a strict JSON object mapping each input string to a grouped label.\n"
            "Inputs:\n" + "\n".join(f"- {name}" for name in chunk)
        )
        payload = {
            "model": model,
            "temperature": 0.1,
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
        }
        resp = requests.post(OPENAI_URL, json=payload, headers=headers, timeout=timeout_s)
        resp.raise_for_status()
        data = resp.json()
        content = data["choices"][0]["message"]["content"].strip()
        chunk_map = _extract_json_mapping(content)
        for name in chunk:
            mapping[name] = chunk_map.get(name, name)
    return mapping


def estimate_hurricane_damage(
    hurricane_name: str,
    state: Optional[str] = None,
    timeout_s: int = 25,
) -> str:
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        raise RuntimeError("OPENAI_API_KEY is not set.")

    model = os.getenv("OPENAI_MODEL", "gpt-4o-mini")
    scope_line = f"State: {state}\n" if state else "State: All affected states\n"
    system_prompt = (
        "You are a disaster analyst. Provide a concise estimate of total damages in USD. "
        "Use cautious language, provide a range if uncertain, and do not invent precise facts. "
        "If you cannot estimate, say so explicitly."
    )
    user_prompt = (
        f"Hurricane: {hurricane_name}\n"
        f"{scope_line}"
        "Return format:\n"
        "1) One short paragraph with an estimated total damage range in USD.\n"
        "2) One line with a single USD range, e.g., \"$10B–$15B (approx)\".\n"
    )
    payload = {
        "model": model,
        "temperature": 0.2,
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


def estimate_hurricane_year_damage(
    hurricane_year: int,
    timeout_s: int = 25,
) -> str:
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        raise RuntimeError("OPENAI_API_KEY is not set.")

    model = os.getenv("OPENAI_MODEL", "gpt-4o-mini")
    system_prompt = (
        "You are a disaster analyst. Provide a concise estimate of total US hurricane damages "
        "for a given year. Use cautious language and do not invent precise facts. "
        "If you cannot estimate, say so explicitly."
    )
    user_prompt = (
        f"Hurricane year: {hurricane_year}\n"
        "Return format:\n"
        "1) One short paragraph with an estimated total damage range in USD.\n"
        "2) One line with a single USD range, e.g., \"$10B–$15B (approx)\".\n"
    )
    payload = {
        "model": model,
        "temperature": 0.2,
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


def summarize_gdelt_headlines(
    hurricane_name: str,
    state: str,
    articles: List[Dict[str, str]],
    timeout_s: int = 25,
) -> str:
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        raise RuntimeError("OPENAI_API_KEY is not set.")

    model = os.getenv("OPENAI_MODEL", "gpt-4o-mini")
    if not articles:
        return "No recent articles found for this hurricane/state."

    article_lines = [
        f"- {a.get('title','').strip()} | {a.get('url','').strip()}"
        for a in articles
        if a.get("title") and a.get("url")
    ]
    system_prompt = (
        "You are a news curator. Select the most relevant headlines for the hurricane and state. "
        "Return a concise bullet list with the title and link. Do not invent sources."
    )
    user_prompt = (
        f"Hurricane: {hurricane_name}\n"
        f"State: {state}\n"
        "Candidate headlines:\n"
        + "\n".join(article_lines)
        + "\n\nReturn up to 3 bullets: '- Title (URL)'."
    )
    payload = {
        "model": model,
        "temperature": 0.2,
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


