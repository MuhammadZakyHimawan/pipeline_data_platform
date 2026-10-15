from src.shared.db import fetch_df
df = fetch_df("SELECT id_kompetitor, nama_hotel, traveloka_object_id FROM public.hotel_master WHERE traveloka_object_id IS NOT NULL AND traveloka_object_id != '' LIMIT 3")
print(f'Jumlah hotel: {len(df)}')
print(df)