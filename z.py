from datetime import datetime

start_date = datetime.strptime("2023-01-30", "%Y-%m-%d")
end_date = datetime.strptime("2023-05-25", "%Y-%m-%d")
n_diff_days = (end_date - start_date).days

freq_map = {
    (366, float('inf')): "Y",    # Yearly for 366 days or more
    (31, 365): "M",              # Monthly for 31-365 days
    (8, 30): "W",                # Weekly for 8-30 days

}

freq = None

for range_key, frequency in freq_map.items():
    if range_key[0] <= n_diff_days < range_key[1]:
        freq = frequency
        break