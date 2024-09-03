from requests import Session
import json

from .utils import apply


def openalex_download(dois: str, per_page: int, sess: Session = Session()):
    response = sess.get(
        "https://api.openalex.org/works",
        params={
            "mailto": "snorkysnark@gmail.com",
            "per_page": per_page,
            "filter": "doi:" + dois,
        },
    )
    response.raise_for_status()
    return {"json": response.text}


def openalex_split(json_str: str):
    for result in json.loads(json_str)["results"]:
        yield {
            "doi": result["doi"].replace("https://doi.org/", ""),
            "json": json.dumps(result),
        }


def openalex_publication_parse(json_str: str):
    data = json.loads(json_str)
    return {
        "openalex": data["id"].replace("https://openalex.org/", ""),
        "authorships": json.dumps(data["authorships"]),
    }


def openalex_authors_parse(json_str: str):
    for authorship in json.loads(json_str):
        yield {
            "openalex": authorship["author"]["id"].replace("https://openalex.org/", ""),
            "display_name": authorship["author"]["display_name"],
            "orcid": apply(
                authorship["author"]["orcid"],
                lambda s: s.replace("https://orcid.org/", ""),
            ),
            "author_position": authorship["author_position"],
            "institutions": json.dumps(authorship["institutions"]),
        }


def openalex_parse_affiliations(json_str: str):
    for order, institution in enumerate(json.loads(json_str)):
        yield {
            "afid": institution["id"].replace("https://openalex.org/", ""),
            "full_address": institution["display_name"],
            "country_code": institution["country_code"],
            "type_institutions": institution["type"],
            "order_": order,
        }
