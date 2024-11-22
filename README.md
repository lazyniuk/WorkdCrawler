# Workday Job Postings Scraper

This script crawls Workday job postings from a list of URLs and filters job postings based on a specified job title and their posting date. The results are saved in a JSON file.

## Usage
1. Rename ```urls.txt.example``` to ```urls.txt```

2. Run the script with the required job title filter:
    ```python scraper.py --title "<JOB TITLE>"```