from __future__ import annotations

import csv
import json
import os
from functools import lru_cache
from pathlib import Path
from typing import Iterable, Tuple, Dict, List, Optional

import requests


OPENAI_URL = "https://api.openai.com/v1/chat/completions"
TRAINING_TSV_PATH = (
    Path(__file__).resolve().parent.parent / "data" / "annual_disaster_theme_training.tsv"
)
def _format_top_states(states: Iterable[Tuple[str, int]]) -> str:
    items = [f"{state} ({count})" for state, count in states]
    return ", ".join(items) if items else "None"


def summarize_bump_entry(
    decade_label: str,
    disaster_type: str,
    top_states: Iterable[Tuple[str, int]],
    binning: str = "decades",
    timeout_s: int = 20,
) -> str:
    api_key = (os.getenv("OPENAI_API_KEY") or "").strip().strip("\"'").strip()
    if not api_key:
        raise RuntimeError("OPENAI_API_KEY is not set.")

    model = os.getenv("OPENAI_MODEL", "gpt-4o-mini")
    top_states_text = _format_top_states(top_states)

    is_year = binning == "years"
    period_label = "year" if is_year else "decade"
    system_prompt = (
        "You are a concise disaster analyst. Provide a broad thematic overview of the "
        f"selected disaster type and {period_label}, highlighting key themes or notable events. "
        "Refer to counts as declared county-level disasters related to this disaster type. "
        "Then summarize the most affected states in bullets, naming notable instances "
        "of the disaster type and impacts where possible. "
        + (
            "For year-level summaries, try to name one notable example from that year if "
            "it is a well-known event; if unsure, say so explicitly. "
            if is_year
            else ""
        )
        + "Use cautious language and do not invent facts."
    )
    user_prompt = (
        f"Disaster type: {disaster_type}\n"
        f"Period ({period_label}): {decade_label}\n"
        "Counts represent declared county-level disasters related to this disaster type.\n"
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


def _extract_json_list(text: str) -> List[Dict[str, object]]:
    start = text.find("[")
    end = text.rfind("]")
    if start == -1 or end == -1 or end <= start:
        raise ValueError("No JSON list found in response.")
    payload = text[start : end + 1]
    data = json.loads(payload)
    if not isinstance(data, list):
        raise ValueError("Expected a JSON list in response.")
    return data


@lru_cache(maxsize=1)
def _load_theme_training_rows() -> List[Dict[str, str]]:
    if not TRAINING_TSV_PATH.exists():
        return []
    rows: List[Dict[str, str]] = []
    with TRAINING_TSV_PATH.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle, delimiter="\t")
        for row in reader:
            if not row:
                continue
            if not (row.get("event_name") or "").strip():
                continue
            rows.append(row)
    return rows


def _build_theme_training_hints(
    max_themes: int = 25,
    max_events_per_theme: int = 3,
    max_named_events: int = 40,
) -> str:
    rows = _load_theme_training_rows()
    if not rows:
        return ""

    theme_examples: Dict[str, List[str]] = {}
    named_events: List[str] = []
    for row in rows:
        event = (row.get("event_name") or "").strip()
        theme = (row.get("disaster_theme") or "").strip()
        if event:
            named_events.append(event)
        if theme:
            theme_examples.setdefault(theme, [])
            if event and event not in theme_examples[theme]:
                theme_examples[theme].append(event)

    theme_items = []
    for theme, events in theme_examples.items():
        if not events:
            continue
        theme_items.append(f"{theme}: {', '.join(events[:max_events_per_theme])}")
    theme_items = theme_items[:max_themes]

    named_unique: List[str] = []
    seen = set()
    for event in named_events:
        if event in seen:
            continue
        seen.add(event)
        named_unique.append(event)
    named_unique = named_unique[:max_named_events]

    hints = []
    if theme_items:
        hints.append("Theme examples from training data: " + "; ".join(theme_items) + ".")
    if named_unique:
        hints.append(
            "Named event examples (may or may not map to a theme): "
            + ", ".join(named_unique)
            + "."
        )
    return " ".join(hints)


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


def group_sankey_names(
    records: List[Dict[str, str]],
    timeout_s: int = 40,
    chunk_size: int = 50,
    progress_callback: Optional[callable] = None,
) -> List[Dict[str, object]]:
    api_key = (os.getenv("OPENAI_API_KEY") or "").strip().strip("\"'").strip()
    if not api_key:
        raise RuntimeError("OPENAI_API_KEY is not set.")

    model = os.getenv("OPENAI_MODEL", "gpt-4o-mini")
    cleaned = [r for r in records if r.get("record_id")]
    if not cleaned:
        return []

    training_hints = _build_theme_training_hints()
    system_prompt = (
        "You classify FEMA disaster records and return a JSON list of objects. "
        "For each record, assign a broad theme for the given year (theme_group), and "
        "also determine whether the record refers to a named event (name_group). "
        "Use the provided record_id. If the name is not clearly a named event, set "
        "is_named_event=false, canonical_event_name=null, and name_group=\"Unnamed\". "
        "If you cannot assign a theme from the input, set theme_group=\"No Theme\". "
        "Theme examples: \"2024 Atlantic Hurricane Season\", "
        "\"Atmospheric River Flooding\", \"Midwest Tornado Outbreak\". "
        "Use these training hints as guidance only (not as required matches): "
        f"{training_hints} "
        "Do not invent specifics beyond the input."
    )

    headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}
    results: List[Dict[str, object]] = []
    for i in range(0, len(cleaned), chunk_size):
        chunk = cleaned[i : i + chunk_size]
        payload_records = [
            {
                "record_id": r.get("record_id"),
                "year": r.get("year"),
                "disaster_type": r.get("disaster_type", ""),
                "declaration_name": r.get("declaration_name", ""),
            }
            for r in chunk
        ]
        user_prompt = (
            "Return a strict JSON list of objects with keys: record_id, "
            "theme_group (string), theme_confidence (0-1), "
            "is_named_event (boolean), canonical_event_name (string or null), "
            "name_group (string), confidence (0-1). "
            "If no theme is clear, use theme_group=\"No Theme\". Input records:\n"
            + json.dumps(payload_records, ensure_ascii=True)
        )
        payload = {
            "model": model,
            "temperature": 0.1,
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
        }
        try:
            resp = requests.post(
                OPENAI_URL, json=payload, headers=headers, timeout=timeout_s
            )
            resp.raise_for_status()
            data = resp.json()
            content = data["choices"][0]["message"]["content"].strip()
            results.extend(_extract_json_list(content))
            if progress_callback:
                progress_callback(len(chunk))
        except (requests.exceptions.ReadTimeout, requests.exceptions.ConnectionError):
            break
    return results


def _format_pairs(items: Iterable[Tuple[str, int]], max_items: int = 8) -> str:
    trimmed = list(items)[:max_items]
    return ", ".join(f"{label} ({count})" for label, count in trimmed) if trimmed else "None"


def summarize_year_events(
    year: int,
    top_types: Iterable[Tuple[str, int]],
    top_events: Iterable[Tuple[str, int]],
    timeout_s: int = 25,
) -> str:
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        raise RuntimeError("OPENAI_API_KEY is not set.")

    model = os.getenv("OPENAI_MODEL", "gpt-4o-mini")
    types_text = _format_pairs(top_types)
    events_text = _format_pairs(top_events)
    system_prompt = (
        "You are a concise disaster analyst. Provide a brief narrative about natural "
        "disasters in the specified year. Use cautious language and do not invent facts."
    )
    user_prompt = (
        f"Year: {year}\n"
        f"Top disaster types (by count): {types_text}\n"
        f"Top named events (by count): {events_text}\n"
        "Return a short paragraph (2-4 sentences)."
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


def summarize_named_event(
    event_name: str,
    year: Optional[int],
    top_states: Iterable[Tuple[str, int]],
    timeout_s: int = 25,
) -> str:
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        raise RuntimeError("OPENAI_API_KEY is not set.")

    model = os.getenv("OPENAI_MODEL", "gpt-4o-mini")
    states_text = _format_pairs(top_states)
    system_prompt = (
        "You are a concise disaster analyst. Provide a brief narrative about the named event. "
        "Use cautious language and do not invent facts."
    )
    user_prompt = (
        f"Named event: {event_name}\n"
        f"Year: {year if year is not None else 'Unknown'}\n"
        f"Top affected states (by count): {states_text}\n"
        "Return a short paragraph (2-4 sentences)."
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


def summarize_unnamed_events(
    year: int,
    top_types: Iterable[Tuple[str, int]],
    timeout_s: int = 25,
) -> str:
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        raise RuntimeError("OPENAI_API_KEY is not set.")

    model = os.getenv("OPENAI_MODEL", "gpt-4o-mini")
    types_text = _format_pairs(top_types)
    system_prompt = (
        "You are a concise disaster analyst. Provide a brief narrative about unnamed events "
        "in the specified year. Use cautious language and do not invent facts."
    )
    user_prompt = (
        f"Year: {year}\n"
        f"Top disaster types among unnamed events (by count): {types_text}\n"
        "Return a short paragraph (2-4 sentences)."
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


def summarize_event_state(
    event_name: str,
    state: str,
    year: Optional[int],
    timeout_s: int = 25,
) -> str:
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        raise RuntimeError("OPENAI_API_KEY is not set.")

    model = os.getenv("OPENAI_MODEL", "gpt-4o-mini")
    system_prompt = (
        "You are a concise disaster analyst. Provide a brief narrative about the named event "
        "impact in the specified state. Use cautious language and do not invent facts."
    )
    user_prompt = (
        f"Named event: {event_name}\n"
        f"State: {state}\n"
        f"Year: {year if year is not None else 'Unknown'}\n"
        "Return a short paragraph (2-4 sentences)."
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


