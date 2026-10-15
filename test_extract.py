from src.extract.traveloka_review_scraper import extract

# Test with row limit = 1 hotel
import src.shared.row_limit as rl
rl.set_row_limit(1)

result = extract(period="2026-01", dry_run=True)
print(result)