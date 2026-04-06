from hardcover_tools.core.matching import (
    bare_title_similarity,
    explain_author_mismatch,
    is_partial_author_match,
    primary_author_overlap,
    title_marketing_penalty,
    title_similarity,
)


def test_title_similarity_is_canonical_for_case_and_punctuation() -> None:
    assert title_similarity("Assassin's Apprentice", "assassins apprentice") == 1.0
    assert bare_title_similarity("Royal Assassin (The Illustrated Edition)", "Royal Assassin") == 1.0


def test_marketing_penalty_flags_fluff_and_clean_titles() -> None:
    assert title_marketing_penalty("Darkest Fear") == 0.0
    assert title_marketing_penalty("Darkest Fear: A Novel") > 0.0
    assert title_marketing_penalty("Dragonfall The Brand New Must-Read Fantasy Debut") > 0.0


def test_partial_author_helpers_preserve_overlap_behavior() -> None:
    assert explain_author_mismatch("Robin Hobb", "Robin Hobb & Stephen Youll") == "candidate_has_extra_primary_authors"
    assert explain_author_mismatch("Robin Hobb & Stephen Youll", "Robin Hobb") == "file_has_extra_primary_authors"
    assert is_partial_author_match("Robin Hobb", "Robin Hobb & Stephen Youll")
    assert primary_author_overlap("Robin Hobb & John Howe", "Robin Hobb")
    assert not primary_author_overlap("Stephen Youll & Robin Hobb", "Robin Hobb")
