import requests
import json
import time
import random
from collections import defaultdict
from urllib.parse import urlparse
import threading
from queue import Queue
import argparse
import os

URLS_FILE = 'urls.txt'
FILTER_KEYWORDS_FILE = 'filter_keywords.txt'
DAYS_THRESHOLD = 7
MAX_REQUESTS_PER_DOMAIN_PER_MINUTE = 60
MAX_CONCURRENT_REQUESTS = 10

filtered_jobs = []
jobs_lock = threading.Lock()
domains_last_access = defaultdict(float)
domains_request_count = defaultdict(int)

headers = {
    'Accept': 'application/json',
    'Content-Type': 'application/json',
    'Accept-Language': 'en-US,en;q=0.9',
    'Accept-Encoding': 'gzip, deflate, br',
}

payload = {
    'appliedFacets': {},
    'limit': 20,
    'offset': 0,
}

def load_filter_keywords():
    if not os.path.exists(FILTER_KEYWORDS_FILE):
        with open(FILTER_KEYWORDS_FILE, 'w') as f:
            pass
    with open(FILTER_KEYWORDS_FILE, 'r') as f:
        filter_keywords = {line.strip().lower() for line in f if line.strip()}
    return filter_keywords

filter_keywords = load_filter_keywords()

def is_posted_recently(posted_on):
    posted_on = posted_on.replace('Posted ', '').strip()
    if 'Days Ago' in posted_on:
        try:
            days = int(posted_on.split(' ')[0])
            return days <= DAYS_THRESHOLD
        except ValueError:
            return False
    if 'Today' in posted_on or 'Yesterday' in posted_on:
        return True
    return False

def parse_posted_date(posted_on):
    posted_on = posted_on.replace('Posted ', '').strip()
    if 'Today' in posted_on:
        return 0
    elif 'Yesterday' in posted_on:
        return 1
    elif 'Days Ago' in posted_on:
        try:
            days = int(posted_on.split(' ')[0])
            return days
        except ValueError:
            return float('inf')
    return float('inf')

def is_job_excluded(job):
    fields_to_check = [job.get('title', ''), job.get('location', ''), job.get('jobUrl', '')]
    for field in fields_to_check:
        if any(keyword in field.lower() for keyword in filter_keywords):
            return True
    return False

def fetch_jobs(session, url, titles):
    base_url = '/'.join(url.split('/')[:3])
    career_site_path = url.split('/')[-1]
    subdomain = base_url.split('//')[1].split('.')[0]
    api_url = f'{base_url}/wday/cxs/{subdomain}/{career_site_path}/jobs'
    referer_url = url
    session.headers.update({
        'Origin': base_url,
        'Referer': referer_url,
    })
    domain = urlparse(url).netloc

    search_text = " OR ".join(titles)
    payload['searchText'] = search_text

    try:
        response = session.post(api_url, json=payload)
        response.raise_for_status()
        job_postings = response.json().get('jobPostings', [])
        for job in job_postings:
            if is_posted_recently(job.get('postedOn', '')):
                for title in titles:
                    if title.lower() in job.get('title', '').lower() and not is_job_excluded({
                        'title': job.get('title', ''),
                        'location': job.get('locationsText', ''),
                        'jobUrl': url + job.get('externalPath')
                    }):
                        filtered_job = {
                            'title': job.get('title'),
                            'location': job.get('locationsText'),
                            'postedOn': job.get('postedOn'),
                            'jobUrl': url + job.get('externalPath'),
                            'searchTitle': title
                        }
                        with jobs_lock:
                            filtered_jobs.append(filtered_job)
        domains_request_count[domain] += 1
        domains_last_access[domain] = time.time()
    except requests.exceptions.RequestException as e:
        print(f"Error fetching jobs from {api_url}: {e}")
    finally:
        time.sleep(random.uniform(1, 3))

def worker(domain_queue, titles):
    session = requests.Session()
    while True:
        item = domain_queue.get()
        if item is None:
            break
        domain, url = item
        fetch_jobs(session, url, titles)
        domain_queue.task_done()

def save_sorted_jobs(output_file):
    with jobs_lock:
        sorted_jobs = sorted(filtered_jobs, key=lambda job: parse_posted_date(job['postedOn']))
    with open(output_file, 'w') as f:
        json.dump(sorted_jobs, f, indent=4)
    print(f'Filtered and sorted jobs saved to {output_file}')

def main():
    parser = argparse.ArgumentParser(description="Crawl Workday job postings.")
    parser.add_argument(
        '-t', '--titles', required=True, help="Comma-separated list of search titles or a file path containing titles."
    )
    parser.add_argument(
        '-o', '--output', required=True, help="Output JSON file for all jobs."
    )
    args = parser.parse_args()

    if os.path.exists(args.titles):
        with open(args.titles, 'r') as f:
            search_titles = [line.strip() for line in f if line.strip()]
    else:
        search_titles = [title.strip() for title in args.titles.split(',')]

    with open(URLS_FILE, 'r') as f:
        urls = [line.strip() for line in f if line.strip()]

    domain_queue = Queue()

    for url in urls:
        domain = urlparse(url).netloc
        domain_queue.put((domain, url))

    threads = []
    for _ in range(min(MAX_CONCURRENT_REQUESTS, len(urls))):
        t = threading.Thread(target=worker, args=(domain_queue, search_titles))
        t.start()
        threads.append(t)

    domain_queue.join()

    for _ in threads:
        domain_queue.put(None)
    for t in threads:
        t.join()

    save_sorted_jobs(args.output)

if __name__ == '__main__':
    main()
