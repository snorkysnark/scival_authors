from requests import Session
import json


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
