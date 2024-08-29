import re

delimiter = re.compile(r"\|\s*")


def split_authors(authors: str, scopus_author_ids: str):
    for name, scopus_id in zip(
        delimiter.split(authors), delimiter.split(scopus_author_ids)
    ):
        yield {
            "author_name": name.replace("\u00A0", " "),
            "scopus_author_id": scopus_id,
        }
