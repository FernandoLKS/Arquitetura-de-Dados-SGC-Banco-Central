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
        timeout=60
    )

    if response.status_code == 404:
        return []

    response.raise_for_status()

    try:
        return response.json()

    except ValueError as error:
        raise RuntimeError(
            "BCB API returned a non-JSON response. "
            f"Series: {series_code}. "
            f"Period: {start_date} -> {end_date}. "
            f"Status: {response.status_code}. "
            f"Content-Type: {response.headers.get('Content-Type')}. "
            f"Response: {response.text[:500]!r}"
        ) from error