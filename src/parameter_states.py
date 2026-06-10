import asyncio, csv, os

DATABASE = []
DB_LOCK = asyncio.Lock()

def write_candidate_results(session_dir_name: str, results_to_append: list):
    DATABASE.extend(results_to_append)

    filename = os.path.join(session_dir_name, "results.csv")
    file_exists = os.path.exists(filename) and os.path.getsize(filename) > 0

    headers = DATABASE[0].keys()
    with open(filename, mode="a", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=headers)
        
        if not file_exists: writer.writeheader()
        writer.writerows(results_to_append)