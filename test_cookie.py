from src.extract.traveloka_review_scraper import _load_cookie_and_token
cookie_str, sen_t_token, cookie_dict = _load_cookie_and_token()
print(f"Cookie str length: {len(cookie_str)}")
print(f"sen_t_token: {sen_t_token[:20] if sen_t_token else 'EMPTY'}")
print(f"Cookie dict keys: {list(cookie_dict.keys())}")
print(f"Critical cookies present: {[c for c in ['sen_t', 'clientSessionId', 'tv_mcc_id'] if c in cookie_dict]}")