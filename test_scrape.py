from src.extract.traveloka_review_scraper import _fetch_all_reviews, _load_cookie_and_token

cookie_str, sen_t_token, cookie_dict = _load_cookie_and_token()

# Test with first hotel
object_id = "9000005590467"
print(f"Testing fetch for object_id: {object_id}")

raw_reviews = _fetch_all_reviews(
    object_id,
    max_pages=1,
    delay=1.0,
    timeout=30,
    cookie_str=cookie_str,
    sen_t_token=sen_t_token,
    cookie_dict=cookie_dict,
)

print(f"Got {len(raw_reviews)} reviews")
for r in raw_reviews[:2]:
    print(f"  - {r.get('reviewId')}: {r.get('reviewTimestamp')}")