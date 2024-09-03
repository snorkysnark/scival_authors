from dataclasses import dataclass
import re

delimiter = re.compile(r"\|\s*")


@dataclass
class Author:
    name: str
    scopus_id: str

    def __eq__(self, value: object, /) -> bool:
        return isinstance(value, Author) and value.scopus_id == self.scopus_id

    def __hash__(self) -> int:
        return hash(self.scopus_id)


def split_authors(authors: str, scopus_author_ids: str):
    # Filter out duplicate scopus_ids
    for author in set(
        Author(*pair)
        for pair in zip(delimiter.split(authors), delimiter.split(scopus_author_ids))
    ):
        yield {
            "author_name": author.name.replace("\u00A0", " "),
            "scopus_author_id": author.scopus_id,
        }
