import requests

# Try different API endpoints
endpoints = [
    "https://www.traveloka.com/api/v2/hotel/search/autoComplete",
    "https://www.traveloka.com/api/v1/hotel/search/autoComplete",
    "https://www.traveloka.com/api/hotel/search/autoComplete",
]

session = requests.Session()
# Set minimal cookies
for k, v in {
    'tv_user': '{"authorizationLevel":"400","id":"417137294"}',
    'countryCode': 'ID',
    'clientSessionId': 'T1-web.01M4AHP0Q69XQ4CSPHACTQGTCS',
    'tv_cs': '1',
    'tv-repeat-visit': 'true',
}.items():
    session.cookies.set(k, v, domain='traveloka.com')

headers = {
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
    'content-type': 'application/json',
    'origin': 'https://www.traveloka.com',
    'referer': 'https://www.traveloka.com/',
    'x-traveloka-app': 'desktop',
    'x-traveloka-locale': 'id-ID',
}

for endpoint in endpoints:
    resp = session.post(
        endpoint,
        json={'clientInterface': 'desktop', 'data': {'query': '38 Prima Hotel'}},
        headers=headers,
        timeout=15
    )
    print(f'{endpoint} -> Status: {resp.status_code}, Body: {resp.text[:200]}')
    if resp.status_code == 200:
        print('SUCCESS!')
        break