import requests

url = "https://data.oireachtas.ie/akn/ie/debateRecord/dail/2025-01-22/writtens/mul@/dbsect_5.xml"

headers = {
    "User-Agent": "OireachtasResearchBot/1.0 (MSc Thesis; contact: B00177505@myTUDublin.ie"
}

resp = requests.get(url, headers=headers, timeout=30)

print(resp.status_code)
print(resp.headers.get("Content-Type"))
print(resp.text[:500])  # preview first 500 chars