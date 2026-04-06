from hardcover_tools.core.edition_selection import (
    choose_preferred_edition_info,
    is_audio_edition,
    is_collectionish_edition,
    is_ebookish_edition,
)
from hardcover_tools.core.models import (
    BookRecord,
    EmbeddedMeta,
    FileWork,
    HardcoverBook,
    HardcoverEdition,
)


def _build_record() -> BookRecord:
    return BookRecord(
        calibre_book_id=1,
        calibre_title="The Test Book",
        calibre_authors="Jane Doe",
        calibre_series="",
        calibre_series_index=None,
        calibre_language="eng",
        calibre_hardcover_id="",
        calibre_hardcover_slug="",
        file_format="EPUB",
    )


def _build_file_work() -> FileWork:
    return FileWork(title="The Test Book", authors="Jane Doe", language="English")


def _build_book(default_ebook_edition_id: int = 0) -> HardcoverBook:
    return HardcoverBook(
        id=1,
        title="The Test Book",
        subtitle="",
        authors="Jane Doe",
        series="",
        release_date="",
        slug="the-test-book",
        default_ebook_edition_id=default_ebook_edition_id,
    )


def test_collectionish_detection_catches_omnibus_and_box_sets() -> None:
    omnibus = HardcoverEdition(
        id=10,
        book_id=1,
        title="The Test Book Omnibus",
        subtitle="Contains Books 1-3",
    )
    normal = HardcoverEdition(
        id=11,
        book_id=1,
        title="The Test Book",
        subtitle="",
    )

    assert is_collectionish_edition(omnibus)
    assert not is_collectionish_edition(normal)


def test_choose_preferred_edition_prefers_english_ebook_when_available() -> None:
    ebook = HardcoverEdition(
        id=10,
        book_id=1,
        title="The Test Book",
        subtitle="",
        authors="Jane Doe",
        edition_format="Kindle",
        reading_format="Ebook",
        language="English",
    )
    paperback = HardcoverEdition(
        id=11,
        book_id=1,
        title="The Test Book",
        subtitle="",
        authors="Jane Doe",
        edition_format="Paperback",
        reading_format="Read",
        language="English",
    )
    audiobook = HardcoverEdition(
        id=12,
        book_id=1,
        title="The Test Book",
        subtitle="",
        authors="Jane Doe",
        edition_format="Audible Audio",
        reading_format="Listened",
        language="English",
        audio_seconds=3600,
    )

    info = choose_preferred_edition_info(
        _build_record(),
        _build_file_work(),
        EmbeddedMeta(),
        _build_book(default_ebook_edition_id=10),
        [audiobook, paperback, ebook],
    )

    assert info.chosen is not None
    assert info.chosen.id == 10
    assert is_ebookish_edition(info.chosen)
    assert not is_audio_edition(info.chosen)


def test_choose_preferred_edition_uses_english_reserve_before_blank_language_ebook() -> None:
    blank_language_ebook = HardcoverEdition(
        id=10,
        book_id=1,
        title="The Test Book",
        subtitle="",
        authors="Jane Doe",
        edition_format="Kindle",
        reading_format="Ebook",
        language="",
    )
    english_paperback = HardcoverEdition(
        id=11,
        book_id=1,
        title="The Test Book",
        subtitle="",
        authors="Jane Doe",
        edition_format="Paperback",
        reading_format="Read",
        language="English",
    )
    audiobook = HardcoverEdition(
        id=12,
        book_id=1,
        title="The Test Book",
        subtitle="",
        authors="Jane Doe",
        edition_format="Audible Audio",
        reading_format="Listened",
        language="English",
        audio_seconds=3600,
    )

    info = choose_preferred_edition_info(
        _build_record(),
        _build_file_work(),
        EmbeddedMeta(),
        _build_book(),
        [audiobook, blank_language_ebook, english_paperback],
    )

    assert info.chosen is not None
    assert info.chosen.id == 11
    assert not is_audio_edition(info.chosen)
