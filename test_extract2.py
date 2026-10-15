import logging
logging.basicConfig(level=logging.INFO)

from src.extract.traveloka_review_scraper import extract, _get_traveloka_hotels, _load_cookie_and_token
from src.shared.config import get_source_config

cfg = get_source_config("traveloka_review")
print("Config loaded")

hotels = _get_traveloka_hotels(cfg)
print(f"Got {len(hotels)} hotels")

import src.shared.row_limit as rl
rl.set_row_limit(1)
print("Row limit set")

cookie_str, sen_t_token, cookie_dict = _load_cookie_and_token()
print("Cookies loaded")

# Call extract with dry_run
print("Calling extract...")
result = extract(period="2026-01", dry_run=True, force_login=False, headless_login=False, keep_browser_open=False)
print(f"Result: {result}")