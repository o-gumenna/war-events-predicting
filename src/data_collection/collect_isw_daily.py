import re
import time
import csv
import json
from pathlib import Path
from playwright.sync_api import sync_playwright
from bs4 import BeautifulSoup
from datetime import date, timedelta

# configuration and constants
catalog_url = "https://understandingwar.org/research/?_teams=russia-ukraine"
base_domain = "https://understandingwar.org"
csv_output_path = "data/isw/isw_reports.csv"
json_output_path = "data/isw/isw_reports.json"
request_delay = 1.5

# regex to find month and day in titles
title_date_regex = re.compile(
    r"(january|february|march|april|may|june|july|august|september|october|november|december)\s+(\d{1,2})",
    re.IGNORECASE,
)

# regex to extract year from page content
year_regex = re.compile(r"\b(20\d{2})\b")

months_map = {
    "january": 1, "february": 2, "march": 3, "april": 4,
    "may": 5, "june": 6, "july": 7, "august": 8,
    "september": 9, "october": 10, "november": 11, "december": 12
}


def scrape_text(soup):
    # extracts main article text and removes unnecessary elements
    content_box = (
        soup.find("div", class_=re.compile(r"field--name-body|field-name-body|field-items")) or
        soup.find("article")
    )
    text = ""
    if content_box:
        for tag in content_box(["script", "style", "nav", "footer"]):
            tag.decompose()
        text = content_box.get_text(separator="\n", strip=True)
    return text


def date_already_collected(target_date):
    # check if a record for the target date already exists in the CSV
    path = Path(csv_output_path)
    if not path.exists():
        return False

    csv.field_size_limit(10_000_000)  # 10MB — enough for any ISW report

    with open(path, "r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row["date"] == str(target_date):
                return True
    return False


def append_to_json(record):
    # append a single record to the JSON file
    path = Path(json_output_path)
    records = []
    if path.exists():
        with open(path, "r", encoding="utf-8") as f:
            records = json.load(f)

    records.append(record)
    records.sort(key=lambda x: x["date"])

    with open(path, "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)


def append_to_csv(record):
    # append a single record to the CSV file
    path = Path(csv_output_path)
    file_exists = path.exists()

    Path(csv_output_path).parent.mkdir(parents=True, exist_ok=True)

    with open(path, "a", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=["date", "url", "full_text", "word_count"],
            quoting=csv.QUOTE_ALL
        )
        # write header only if file is new
        if not file_exists:
            writer.writeheader()
        writer.writerow(record)


def main():
    # target date: yesterday (ISW publishes reports for the previous day)
    target_date = date.today() - timedelta(days=1)
    print(f"collecting ISW report for: {target_date}")

    # check if already collected
    if date_already_collected(target_date):
        print(f"report for {target_date} already exists in {csv_output_path}. skipping.")
        return

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True)
        context = browser.new_context(user_agent="Mozilla/5.0")
        page = context.new_page()

        found_url = None

        print("searching catalog for today's report...")

        # check first 3 pages in case of publishing delay
        for pg in range(1, 4):
            try:
                page.goto(f"{catalog_url}&_paged={pg}", wait_until="networkidle", timeout=60000)
                soup = BeautifulSoup(page.content(), "html.parser")

                for a_tag in soup.find_all("a", href=True):
                    title_text = a_tag.get_text(strip=True)
                    if "Russian Offensive Campaign Assessment" not in title_text:
                        continue

                    date_match = title_date_regex.search(title_text)
                    if not date_match:
                        continue

                    month_num = months_map[date_match.group(1).lower()]
                    day = int(date_match.group(2))

                    if month_num == target_date.month and day == target_date.day:
                        href = a_tag["href"].split("?")[0].rstrip("/")
                        found_url = base_domain + href if href.startswith("/") else href
                        print(f"found: {found_url}")
                        break

                if found_url:
                    break

            except Exception as e:
                print(f"error on catalog page {pg}: {e}")

            time.sleep(request_delay)

        if not found_url:
            print(f"no report found for {target_date}. it may not be published yet.")
            browser.close()
            return

        # scrape the report page
        try:
            page.goto(found_url, wait_until="domcontentloaded", timeout=60000)
            soup = BeautifulSoup(page.content(), "html.parser")

            # find the year in the article body or subheaders
            year = None
            for el in soup.find_all(string=year_regex):
                match = year_regex.search(el)
                if match:
                    year = int(match.group(1))
                    break

            if year is None:
                print("skipped (no year found)")
                browser.close()
                return

            report_date = date(year, target_date.month, target_date.day)

            if report_date != target_date:
                print(f"date mismatch: expected {target_date}, got {report_date}. skipping.")
                browser.close()
                return

            text = scrape_text(soup)
            record = {
                "date": str(report_date),
                "url": found_url,
                "full_text": text,
                "word_count": len(text.split())
            }

            append_to_csv(record)
            append_to_json(record)
            print(f"success! appended report for {report_date} to CSV and JSON")
            print(f"word count: {record['word_count']}")

        except Exception as e:
            print(f"failed to scrape {found_url}: {e}")

        browser.close()


if __name__ == "__main__":
    main()