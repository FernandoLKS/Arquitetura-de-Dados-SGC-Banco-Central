import requests


BASE_URL = "https://api.bcb.gov.br/dados/serie/bcdata.sgs"


def get_series(
    series_code: int,
    start_date: str,
    end_date: str
):

    url = f"{BASE_URL}.{series_code}/dados"

    params = {
        "formato": "json",
        "dataInicial": start_date,
        "dataFinal": end_date
    }

    response = requests.get(
        url,
        params=params,
        timeout=30
    )

    if response.status_code == 404:
        return []

    response.raise_for_status()

    return response.json()

    return response.json()