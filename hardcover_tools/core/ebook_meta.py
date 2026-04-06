from __future__ import annotations

import html
import re
import shlex
import shutil
import subprocess
import zipfile
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from xml.etree import ElementTree as ET

from .language import looks_english
from .matching import bare_title_similarity
from .models import BookRecord, ContentSignals, EmbeddedMeta, FileWork
from .identifiers import clean_isbn
from .text_normalization import (
    norm,
    normalize_author_string,
    normalize_person_name,
    smart_title,
    split_author_like_string,
)

CONTAINER_NS = {"container": "urn:oasis:names:tc:opendocument:xmlns:container"}

IGNORE_CONTENT_TITLES = {
    "cover",
    "title page",
    "copyright",
    "contents",
    "toc",
    "table of contents",
    "full page image",
    "unknown",
    "brand page",
    "dedication",
    "other titles",
    "front endpapers",
    "part0001",
    "cover the",
    "also by",
    "cover,",
}


class EbookMetaRunner:
    def __init__(
        self,
        library_root: Path,
        ebook_meta_command: Optional[str] = None,
        docker_container_name: Optional[str] = None,
        container_library_root: Optional[str] = None,
        host_timeout: int = 15,
        docker_timeout: int = 20,
    ) -> None:
        self.library_root = library_root.resolve()
        self.ebook_meta_command = ebook_meta_command
        self.docker_container_name = docker_container_name
        self.container_library_root = container_library_root or "/calibre-library"
        self.host_timeout = max(1, int(host_timeout or 15))
        self.docker_timeout = max(1, int(docker_timeout or 20))

    def _host_command(self) -> Optional[List[str]]:
        if self.ebook_meta_command:
            return shlex.split(self.ebook_meta_command)
        host = shutil.which("ebook-meta")
        return [host] if host else None

    def _docker_command(self, host_file_path: Path) -> Optional[List[str]]:
        if not self.docker_container_name:
            return None
        try:
            relative = host_file_path.resolve().relative_to(self.library_root)
        except Exception:
            return None
        container_path = str(Path(self.container_library_root) / relative)
        command = f"ebook-meta {shlex.quote(container_path)}"
        return ["docker", "exec", "-i", self.docker_container_name, "sh", "-lc", command]

    def run(self, host_file_path: Path) -> EmbeddedMeta:
        if not host_file_path.exists():
            return EmbeddedMeta(tool_used="missing_file", parse_error="file does not exist")

        extension = host_file_path.suffix.lower()
        if extension in {".epub", ".kepub", ".oebzip"}:
            opf = parse_epub_opf_metadata(host_file_path)
            if opf.embedded_title or opf.embedded_authors or opf.embedded_identifiers:
                opf.tool_used = "epub-opf-fastpath"
                return opf

        host_command = self._host_command()
        if host_command:
            try:
                process = subprocess.run(
                    host_command + [str(host_file_path)],
                    capture_output=True,
                    text=True,
                    timeout=self.host_timeout,
                    check=False,
                )
                if process.returncode == 0:
                    return parse_ebook_meta_output(
                        (process.stdout or "") + "\n" + (process.stderr or ""),
                        "host-ebook-meta",
                    )
            except Exception:
                pass

        docker_command = self._docker_command(host_file_path)
        if docker_command:
            try:
                process = subprocess.run(
                    docker_command,
                    capture_output=True,
                    text=True,
                    timeout=self.docker_timeout,
                    check=False,
                )
                if process.returncode == 0:
                    return parse_ebook_meta_output(
                        (process.stdout or "") + "\n" + (process.stderr or ""),
                        f"docker:{self.docker_container_name}",
                    )
                return EmbeddedMeta(
                    tool_used=f"docker:{self.docker_container_name}",
                    parse_error=(process.stderr or "")[:500],
                )
            except Exception as exc:
                return EmbeddedMeta(tool_used=f"docker:{self.docker_container_name}", parse_error=str(exc))

        return EmbeddedMeta(tool_used="none", parse_error="ebook-meta unavailable")


def parse_ebook_meta_output(text: str, tool_used: str) -> EmbeddedMeta:
    metadata = EmbeddedMeta(raw=text, tool_used=tool_used)
    identifiers: Dict[str, str] = {}
    for line in text.splitlines():
        if ":" not in line:
            continue
        key, value = line.split(":", 1)
        key = key.strip().lower()
        value = value.strip()
        if not value:
            continue
        if key.startswith("title") and not metadata.embedded_title:
            metadata.embedded_title = smart_title(value)
        elif key.startswith("author(s)") or key == "authors":
            metadata.embedded_authors = normalize_author_string(value)
        elif key.startswith("languages"):
            metadata.embedded_language = value
        elif key.startswith("identifiers"):
            for part in re.split(r",\s*", value):
                if ":" in part:
                    name, identifier_value = part.split(":", 1)
                    identifiers[name.strip().lower()] = identifier_value.strip()
        elif key.startswith("isbn"):
            identifiers["isbn"] = clean_isbn(value)
    metadata.embedded_identifiers = identifiers
    return metadata


def parse_epub_opf_metadata(file_path: Path) -> EmbeddedMeta:
    output = EmbeddedMeta(tool_used="epub-opf-fallback")
    try:
        with zipfile.ZipFile(file_path) as archive:
            names = archive.namelist()
            if "META-INF/container.xml" not in names:
                output.parse_error = "container.xml not found"
                return output
            root = ET.fromstring(archive.read("META-INF/container.xml"))
            element = root.find(".//container:rootfile", CONTAINER_NS)
            opf_path = element.attrib.get("full-path") if element is not None else None
            if not opf_path or opf_path not in names:
                output.parse_error = "OPF path not found"
                return output
            opf = archive.read(opf_path).decode("utf-8", errors="ignore")

            def first(pattern: str) -> str:
                match = re.search(pattern, opf, re.I | re.S)
                return smart_title(re.sub(r"\s+", " ", match.group(1)).strip()) if match else ""

            output.embedded_title = first(r"<dc:title[^>]*>(.*?)</dc:title>")
            creators = [
                re.sub(r"\s+", " ", match.group(1)).strip()
                for match in re.finditer(r"<dc:creator[^>]*>(.*?)</dc:creator>", opf, re.I | re.S)
            ]
            output.embedded_authors = normalize_author_string(" & ".join([creator for creator in creators if creator]))
            output.embedded_language = first(r"<dc:language[^>]*>(.*?)</dc:language>")
            identifiers: Dict[str, str] = {}
            for match in re.finditer(r"<dc:identifier[^>]*>(.*?)</dc:identifier>", opf, re.I | re.S):
                value = re.sub(r"\s+", " ", match.group(1)).strip()
                cleaned = clean_isbn(value)
                if len(cleaned) in (10, 13):
                    identifiers["isbn"] = cleaned
            output.embedded_identifiers = identifiers
            return output
    except Exception as exc:
        output.parse_error = str(exc)
        return output


def _read_text_file(path: Path, limit: int = 20000) -> str:
    try:
        return path.read_text(encoding="utf-8", errors="ignore")[:limit]
    except Exception:
        try:
            return path.read_text(encoding="latin-1", errors="ignore")[:limit]
        except Exception:
            return ""


def _strip_tags(text: str) -> str:
    text = re.sub(r"(?is)<script.*?>.*?</script>", " ", text)
    text = re.sub(r"(?is)<style.*?>.*?</style>", " ", text)
    text = re.sub(r"(?s)<[^>]+>", " ", text)
    text = html.unescape(text)
    return re.sub(r"\s+", " ", text).strip()


def clean_content_title_hint(title: str) -> str:
    current = smart_title(title)
    if not current:
        return ""
    normalized = norm(current)
    if normalized in IGNORE_CONTENT_TITLES:
        return ""
    if re.fullmatch(r"part\d{4,}", normalized):
        return ""
    if re.fullmatch(r"97[89]\d{10}.*", normalized):
        return ""
    return current


def extract_epub_text(path: Path, limit: int = 30000) -> Tuple[str, str]:
    sample_parts: List[str] = []
    heading = ""
    try:
        with zipfile.ZipFile(path) as archive:
            names = archive.namelist()
            opf_path = None
            if "META-INF/container.xml" in names:
                root = ET.fromstring(archive.read("META-INF/container.xml"))
                element = root.find(".//container:rootfile", CONTAINER_NS)
                if element is not None:
                    opf_path = element.attrib.get("full-path")
            if not opf_path:
                opf_candidates = [name for name in names if name.lower().endswith(".opf")]
                opf_path = opf_candidates[0] if opf_candidates else None
            spine_hrefs: List[str] = []
            if opf_path and opf_path in names:
                opf_root = ET.fromstring(archive.read(opf_path))
                manifest: Dict[str, str] = {}
                for item in opf_root.findall(".//{http://www.idpf.org/2007/opf}manifest/{http://www.idpf.org/2007/opf}item"):
                    manifest[item.attrib.get("id")] = item.attrib.get("href")
                base = str(Path(opf_path).parent)
                for itemref in opf_root.findall(".//{http://www.idpf.org/2007/opf}spine/{http://www.idpf.org/2007/opf}itemref"):
                    href = manifest.get(itemref.attrib.get("idref"))
                    if href:
                        spine_hrefs.append(str((Path(base) / href).as_posix()).lstrip("./"))
            candidates = spine_hrefs or sorted(
                [name for name in names if re.search(r"\.(xhtml|html|htm|xml)$", name, re.I)]
            )[:5]
            for name in candidates[:5]:
                if name not in names:
                    matches = [candidate for candidate in names if candidate.endswith(name)]
                    if matches:
                        name = matches[0]
                    else:
                        continue
                raw = archive.read(name).decode("utf-8", errors="ignore")
                if not heading:
                    match = re.search(r"(?is)<title>(.*?)</title>", raw) or re.search(r"(?is)<h1[^>]*>(.*?)</h1>", raw)
                    if match:
                        heading = clean_content_title_hint(_strip_tags(match.group(1)))
                text = _strip_tags(raw)
                if text:
                    sample_parts.append(text)
                if sum(len(part) for part in sample_parts) >= limit:
                    break
    except Exception:
        return ("", "")
    return (" ".join(sample_parts)[:limit], heading[:300])


def extract_docx_text(path: Path, limit: int = 30000) -> Tuple[str, str]:
    try:
        with zipfile.ZipFile(path) as archive:
            raw = archive.read("word/document.xml").decode("utf-8", errors="ignore")
            text = _strip_tags(raw)
            lines = [line.strip() for line in re.split(r"[\r\n]+", text) if line.strip()]
            heading = clean_content_title_hint(lines[0][:300] if lines else "")
            return (text[:limit], heading)
    except Exception:
        return ("", "")


def build_text_probe(text: str, slice_chars: int = 4000) -> str:
    current = (text or "").strip()
    if not current:
        return ""
    if len(current) <= slice_chars * 2:
        return current[: slice_chars * 2]
    offsets = [0, max(0, len(current) // 2 - slice_chars // 2), max(0, len(current) - slice_chars)]
    parts: List[str] = []
    seen: set[int] = set()
    for start in offsets:
        start = max(0, min(start, max(0, len(current) - slice_chars)))
        if start in seen:
            continue
        seen.add(start)
        part = current[start : start + slice_chars].strip()
        if part:
            parts.append(part)
    return " ".join(parts)


def extract_content_signals(file_path: str, calibre_title: str, calibre_authors: str) -> ContentSignals:
    path = Path(file_path)
    if not path.exists():
        return ContentSignals(extractor="missing_file")

    extension = path.suffix.lower()
    sample = ""
    heading = ""
    extractor = ""
    language_probe = ""
    try:
        if extension in {".epub", ".kepub", ".oebzip"}:
            sample, heading = extract_epub_text(path)
            language_probe = build_text_probe(sample)
            extractor = "epub"
        elif extension in {".txt", ".text"}:
            raw = _read_text_file(path, 120000)
            sample = raw[:30000]
            lines = [line.strip() for line in raw.splitlines() if line.strip()]
            heading = clean_content_title_hint(lines[0][:300] if lines else "")
            language_probe = build_text_probe(raw)
            extractor = "txt"
        elif extension in {".html", ".htm", ".xhtml", ".xml"}:
            raw = _read_text_file(path, 120000)
            stripped = _strip_tags(raw)
            sample = stripped[:30000]
            match = re.search(r"(?is)<title>(.*?)</title>", raw)
            heading = clean_content_title_hint(_strip_tags(match.group(1)) if match else "")
            language_probe = build_text_probe(stripped)
            extractor = "html"
        elif extension == ".docx":
            full_text, heading = extract_docx_text(path, limit=120000)
            sample = full_text[:30000]
            language_probe = build_text_probe(full_text)
            extractor = "docx"
        else:
            return ContentSignals(extractor=f"unsupported:{extension.lstrip('.')}")
    except Exception as exc:
        return ContentSignals(extractor=f"extract-error:{exc}")

    language, language_score = looks_english(language_probe or sample)
    sample_norm = norm(sample[:10000])
    content_title_probe = heading or sample[:250]
    title_strength = (
        bare_title_similarity(calibre_title, content_title_probe) if calibre_title and content_title_probe else 0.0
    )
    author_strength = max(
        (1.0 if norm(author) and norm(author) in sample_norm else 0.0 for author in [calibre_authors] if author),
        default=0.0,
    )
    inferred_author = ""
    match = re.search(r"\bby\s+([A-Z][A-Za-z.\- ]{2,80})", sample[:3000], re.I)
    if match:
        inferred_author = normalize_person_name(match.group(1).strip())

    return ContentSignals(
        inferred_title_from_content=(heading or "")[:300],
        inferred_author_from_content=inferred_author[:300],
        inferred_language_from_content=language,
        content_title_match_strength=round(title_strength, 3),
        content_author_match_strength=round(author_strength, 3),
        first_heading_excerpt=(heading or sample[:250])[:300],
        extracted_sample_len=len(sample),
        extractor=extractor,
        language_confidence=round(language_score, 4),
    )


def derive_file_work(record: BookRecord, embedded: EmbeddedMeta, content: ContentSignals) -> FileWork:
    title = ""
    title_basis = ""
    for candidate, basis in [
        (embedded.embedded_title, "embedded"),
        (content.inferred_title_from_content, "content"),
        (Path(record.file_path).stem.replace("_", " "), "filename"),
        (record.calibre_title, "calibre_fallback"),
    ]:
        current = clean_content_title_hint(candidate) if basis == "content" else smart_title(candidate)
        if current:
            title = current
            title_basis = basis
            break

    def looks_reasonable_content_author(author_text: str) -> bool:
        parts = split_author_like_string(author_text)
        if not parts:
            return False
        if len(parts) > 4:
            return False
        if any(len(part) > 60 for part in parts):
            return False
        return True

    authors = ""
    authors_basis = ""
    embedded_authors = normalize_author_string(embedded.embedded_authors)
    content_authors = normalize_author_string(content.inferred_author_from_content)
    calibre_authors = normalize_author_string(record.calibre_authors)

    if embedded_authors:
        authors = embedded_authors
        authors_basis = "embedded"
    elif content_authors and looks_reasonable_content_author(content_authors):
        authors = content_authors
        authors_basis = "content"
    elif calibre_authors:
        authors = calibre_authors
        authors_basis = "calibre_fallback"
    elif content_authors:
        authors = content_authors
        authors_basis = "content"

    language = embedded.embedded_language or content.inferred_language_from_content or record.calibre_language
    return FileWork(
        title=title,
        authors=authors,
        language=language,
        title_basis=title_basis,
        authors_basis=authors_basis,
    )
