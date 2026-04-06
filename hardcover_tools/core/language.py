from __future__ import annotations

import re
from typing import Tuple

from .text_normalization import norm

EN_STOPWORDS = {
    "the",
    "and",
    "of",
    "to",
    "a",
    "in",
    "is",
    "it",
    "that",
    "for",
    "you",
    "with",
    "on",
    "as",
    "this",
    "be",
    "are",
    "was",
    "by",
    "or",
    "an",
    "at",
    "from",
    "his",
    "her",
    "their",
    "but",
    "not",
    "have",
    "has",
    "had",
    "he",
    "she",
    "they",
    "we",
    "i",
    "my",
    "me",
    "our",
    "your",
    "who",
    "what",
    "when",
    "where",
    "why",
    "how",
    "said",
    "chapter",
    "prologue",
    "epilogue",
}
DE_STOPWORDS = {"der", "die", "das", "und", "ist", "nicht", "ein", "eine", "mit", "zu", "von", "den", "im", "auf", "für", "sie", "ich"}
FR_STOPWORDS = {"le", "la", "les", "et", "est", "une", "un", "des", "dans", "pour", "pas", "que", "qui", "sur", "avec"}
ES_STOPWORDS = {"el", "la", "los", "las", "y", "es", "una", "un", "de", "que", "en", "para", "con", "por", "no", "como"}


def looks_english(text: str) -> Tuple[str, float]:
    sample = (text or "")[:20000].lower()
    words = re.findall(r"[a-zA-ZÀ-ÿ']+", sample)
    if len(words) < 50:
        return ("uncertain", 0.0)
    total = len(words)

    def hit_rate(vocab: set[str]) -> float:
        return sum(1 for word in words if word in vocab) / total

    english = hit_rate(EN_STOPWORDS)
    german = hit_rate(DE_STOPWORDS)
    french = hit_rate(FR_STOPWORDS)
    spanish = hit_rate(ES_STOPWORDS)
    rates = {"eng": english, "deu": german, "fra": french, "spa": spanish}
    language, score = max(rates.items(), key=lambda item: item[1])
    if score < 0.015:
        return ("uncertain", score)
    if language == "eng" and english >= max(german, french, spanish) * 1.5:
        return ("eng", english)
    if language != "eng" and score >= english * 1.2:
        return (language, score)
    return ("uncertain", score)


def normalize_language_signal(value: str) -> str:
    normalized = norm(value)
    if not normalized:
        return ""
    mapping = {
        "eng": "eng",
        "english": "eng",
        "deu": "deu",
        "de": "deu",
        "german": "deu",
        "deutsch": "deu",
        "fra": "fra",
        "fre": "fra",
        "fr": "fra",
        "french": "fra",
        "francais": "fra",
        "francais france": "fra",
        "spa": "spa",
        "es": "spa",
        "spanish": "spa",
        "espanol": "spa",
        "uncertain": "uncertain",
    }
    return mapping.get(normalized, normalized)


def is_non_english_signal(value: str) -> bool:
    return normalize_language_signal(value) in {"deu", "fra", "spa"}


def looks_englishish_text(text: str) -> bool:
    sample = (text or "").strip()
    if not sample:
        return True
    ascii_letters = sum(1 for ch in sample if ch.isascii() and ch.isalpha())
    alpha = sum(1 for ch in sample if ch.isalpha())
    if alpha and (ascii_letters / alpha) < 0.75:
        return False
    language, score = looks_english(sample)
    if language == "eng":
        return True
    if language in {"deu", "fra", "spa"} and score >= 0.02:
        return False
    return True
