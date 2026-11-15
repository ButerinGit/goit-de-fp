
import csv
import re
from pathlib import Path


# Source CSV file
CSV_PATH = Path(__file__).resolve().parents[1] / "data" / "athlete_bio.csv"

# Match numeric weight ranges such as 70-74 or 102-105
range_pattern = re.compile(
    r"^\s*\d+(?:\.\d+)?\s*[-–—]\s*\d+(?:\.\d+)?\s*$"
)

total_rows = 0
rows_with_dash = 0
numeric_ranges = 0
examples = []

with open(CSV_PATH, encoding="utf-8-sig", newline="") as file:
    reader = csv.DictReader(file)

    for row in reader:
        total_rows += 1
        weight = (row["weight"] or "").strip()

        if any(dash in weight for dash in "-–—"):
            rows_with_dash += 1

            if range_pattern.fullmatch(weight):
                numeric_ranges += 1

            if len(examples) < 10:
                examples.append(weight)

print(f"Total athlete records: {total_rows}")
print(f"Weight values containing a dash: {rows_with_dash}")
print(f"Valid numeric weight ranges: {numeric_ranges}")
print(f"Percentage with a dash: {rows_with_dash / total_rows * 100:.2f}%")

print("\nExamples:")
for weight in examples:
    print(weight)
