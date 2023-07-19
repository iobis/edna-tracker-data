from __future__ import annotations
import requests
import json
import logging
import os
from dotenv import load_dotenv
import requests_cache
from dataclasses import dataclass, asdict, field


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
session = requests_cache.CachedSession(expire_after=60*15)
load_dotenv()
token = None


@dataclass
class Site():
    name: str | None = None
    plutof_id: int | None = None
    locality: str | None = None


@dataclass
class Dna():
    plutof_id: int | None = None
    concentration: float | None = None


@dataclass
class Sample():
    plutof_id: int | None = None
    blank: bool | None = False
    name: str | None = None
    size: float | None = None
    event_plutof_id: int | None = None
    event_begin: str | None = None
    area_plutof_id: int | None = None
    area_locality: str | None = None
    area_name: str | None = None
    area_longitude: float | None = None
    area_latitude: float | None = None
    area_uncertainty: float | None = None
    parent_area_plutof_id: int | None = None
    parent_area_locality: str | None = None
    parent_area_name: str | None = None
    dnas: list[Dna] = field(default_factory=list)


def get_token() -> str:
    global token
    if token is None:
        logger.info(f"Getting token for {os.environ.get('PLUTOF_USER')}")
        url = "https://api.plutof.ut.ee/v1/public/auth/token/"
        res = requests.post(url, data={
            "username": os.environ.get("PLUTOF_USER"),
            "password": os.environ.get("PLUTOF_PASSWORD"),
            "client_id": os.environ.get("PLUTOF_CLIENT_ID"),
            "client_secret": os.environ.get("PLUTOF_CLIENT_SECRET"),
            "grant_type": "password"
        })
        data = res.json()
        token = data["access_token"]
    return token


def paginate(url: str, use_data: bool=False) -> list:
    token = get_token()
    page = 1
    items = []
    while True:
        page_url = url + str(page)
        logger.debug(page_url)
        res = session.get(page_url, headers={
            "Authorization": f"Bearer {token}",
            "User-Agent": "eDNA sample tracker"
        })
        if res.status_code != 200:
            break
        else:
            results = res.json()
            if use_data:
                results = results["data"]
            if type(results) is not list:
                results = results["results"]
            if len(results) == 0:
                break
            items = items + results
            page = page + 1
    return items


def get_object(url: str) -> dict | None:
    token = get_token()
    logger.debug(url)
    res = session.get(url, headers={
        "Authorization": f"Bearer {token}",
        "User-Agent": "eDNA sample tracker"
    })
    if res.status_code != 200:
        return None
    else:
        result = res.json()
        return result


def fetch_samples() -> list[dict]:
    url_samples = "https://api.plutof.ut.ee/v1/taxonoccurrence/materialsample/materialsamples/?format=json&page_size=100&study=108275&page="
    samples = paginate(url_samples)
    samples = sorted(samples, key=lambda sample: sample["name"])
    return samples


def fetch_blank_samples() -> set:
    url_samples = "https://api.plutof.ut.ee/v1/taxonoccurrence/materialsample/materialsamples/search/?page_size=100&study=108275&684=true&include_cb=true&page="
    samples = paginate(url_samples)
    samples = sorted(samples, key=lambda sample: sample["name"])
    return set([sample["id"] for sample in samples])


def fetch_events_for_samples(samples: list[dict]) -> dict[int, dict]:
    events = dict()
    sampling_event_urls = set((sample["samplingevent"] for sample in samples))
    for url in sampling_event_urls:
        event = get_object(url)
        if event is not None:
            events[event["id"]] = event
    return events


def fetch_dnas_for_samples(samples: list[dict]) -> dict[int, dict]:
    dna_dict = dict()
    sample_ids = set((sample["id"] for sample in samples))
    for sample_id in sample_ids:
        url  = f"https://api.plutof.ut.ee/v1/dna-lab/dnas/?include=dna_extraction&material_sample={sample_id}&ordering=-id&page[size]=20&page[number]="
        dnas = paginate(url, use_data=True)
        if len(dnas) > 0:
            dna_dict[sample_id] = dnas
    return dna_dict


def fetch_areas_for_events(events: list[dict]) -> dict[int, dict]:
    areas = dict()
    area_urls = set([event["samplingarea"] for event in events if "samplingarea" in event and event["samplingarea"] is not None])
    for url in area_urls:
        area = get_object(url)
        if area is not None:
            areas[area["id"]] = area
    return areas


def fetch_parent_areas_for_areas(areas: list[dict]) -> dict[int, dict]:
    parent_areas = dict()
    parent_area_urls = set([area["parent_samplingarea"] for area in areas if "parent_samplingarea" in area and area["parent_samplingarea"] is not None])
    for url in parent_area_urls:
        parent_area = get_object(url)
        if parent_area is not None:
            del parent_area["geom"]
            parent_areas[parent_area["id"]] = parent_area
    return parent_areas


def find_id(url: str) -> int:
    return int(url.split("/")[-2])


def main():

    result_samples = []
    result_sites = []

    samples = fetch_samples()
    blank_samples = fetch_blank_samples()
    events_dict = fetch_events_for_samples(samples)
    areas_dict = fetch_areas_for_events(list(events_dict.values()))
    parent_areas_dict = fetch_parent_areas_for_areas(list(areas_dict.values()))
    dnas_dict = fetch_dnas_for_samples(samples)

    for area in parent_areas_dict.values():
        result = Site()
        result.name = area["name"]
        result.plutof_id = area["id"]
        result.locality = area["locality_text"]
        result_sites.append(result)

    for sample in samples:

        result = Sample()
        result.plutof_id = sample["id"]
        result.name = sample["name"]
        result.size = float(sample["size"]) if sample["size"] != "" else None

        if sample["id"] in blank_samples:
            result.blank = True

        if sample["id"] in dnas_dict:
            for dna in dnas_dict[sample["id"]]:
                dna_result = Dna()
                dna_result.plutof_id = dna["id"]
                dna_result.concentration = float(dna["attributes"]["concentration"]) if dna["attributes"]["concentration"] != "" else None
                result.dnas.append(dna_result)

        if "samplingevent" in sample and sample["samplingevent"] is not None:
            samplingevent_id = find_id(sample["samplingevent"])
            samplingevent = events_dict[samplingevent_id]

            result.event_plutof_id = samplingevent["id"]
            result.event_begin = samplingevent["timespan_begin"]

            if "samplingarea" in samplingevent and samplingevent["samplingarea"] is not None:
                samplingarea_id = find_id(samplingevent["samplingarea"])
                samplingarea = areas_dict[samplingarea_id]

                result.area_plutof_id = samplingarea["id"]
                result.area_locality = samplingarea["locality_text"]
                result.area_name = samplingarea["name"]
                result.area_longitude = float(samplingarea["longitude"]) if samplingarea["longitude"] != "" else None
                result.area_latitude = float(samplingarea["latitude"]) if samplingarea["latitude"] != "" else None
                result.area_uncertainty = samplingarea["coordinate_uncertainty_in_meters"]

                if "parent_samplingarea" in samplingarea and samplingarea["parent_samplingarea"] is not None:
                    parent_samplingarea_id = find_id(samplingarea["parent_samplingarea"])
                    parent_samplingarea = parent_areas_dict[parent_samplingarea_id]

                    result.parent_area_plutof_id = parent_samplingarea["id"]
                    result.parent_area_locality = parent_samplingarea["locality_text"]
                    result.parent_area_name = parent_samplingarea["name"]

                else:
                    logger.error(f"Parent sampling area not found for sample {sample['name']}")
            else:
                logger.error(f"Parent sampling area not found for sample {sample['name']}")
        else:
            logger.error(f"Sampling event not found for sample {sample['name']}")

        result_samples.append(result)

    with open("data.json", "w") as data_file:
        json.dump({
            "sites": [asdict(result) for result in result_sites],
            "samples": [asdict(result) for result in result_samples],
        }, data_file, indent=2)


if __name__ == "__main__":
    main()
