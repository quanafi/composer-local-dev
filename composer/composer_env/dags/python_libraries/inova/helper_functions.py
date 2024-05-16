import requests
from requests.adapters import HTTPAdapter, Retry

def http_session():
    retry_strategy = Retry(
        total=3,
        status_forcelist=[429, 500, 502, 504],
        allowed_methods=["GET"],
        raise_on_status=False
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    http = requests.Session()
    http.mount("https://", adapter)
    http.mount("http://", adapter)
    return http