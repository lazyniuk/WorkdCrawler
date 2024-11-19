import requests
import json
import time
import random
from collections import defaultdict
from urllib.parse import urlparse
import threading
from queue import Queue
import argparse

URLS_FILE = 'urls.txt'
DAYS_THRESHOLD = 2
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

def fetch_jobs(session, url, search_text):
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

    payload['searchText'] = search_text

    try:
        response = session.post(api_url, json=payload)
        response.raise_for_status()
        job_postings = response.json().get('jobPostings', [])
        recent_jobs = [
            {
                'title': job.get('title'),
                'location': job.get('locationsText'),
                'postedOn': job.get('postedOn'),
                'jobUrl': url + job.get('externalPath')
            }
            for job in job_postings
            if is_posted_recently(job.get('postedOn', ''))
            and search_text.lower() in job.get('title', '').lower()
        ]

        with jobs_lock:
            filtered_jobs.extend(recent_jobs)

        domains_request_count[domain] += 1
        domains_last_access[domain] = time.time()

    except requests.exceptions.RequestException as e:
        print(f'Error fetching jobs from {api_url}: {e}')
    finally:
        time.sleep(random.uniform(1, 3))



def worker(domain_queue, search_text):
    session = requests.Session()
    while True:
        item = domain_queue.get()
        if item is None:
            break
        domain, url = item
        fetch_jobs(session, url, search_text)
        domain_queue.task_done()

def main():
    parser = argparse.ArgumentParser(description="Crawl Workday job postings.")
    parser.add_argument('-t', '--title', required=True, help="The search text to filter job titles.")
    args = parser.parse_args()

    search_text = args.title

    output_file = f'{search_text.lower().replace(" ", "_")}_jobs.json'

    with open(URLS_FILE, 'r') as f:
        urls = [line.strip() for line in f if line.strip()]

    domain_queue = Queue()
    for url in urls:
        domain = urlparse(url).netloc
        domain_queue.put((domain, url))

    threads = []
    for _ in range(min(MAX_CONCURRENT_REQUESTS, len(urls))):
        t = threading.Thread(target=worker, args=(domain_queue, search_text))
        t.start()
        threads.append(t)

    domain_queue.join()

    for _ in threads:
        domain_queue.put(None)
    for t in threads:
        t.join()

    with open(output_file, 'w') as f:
        json.dump(filtered_jobs, f, indent=4)

    print(f'Filtered jobs saved to {output_file}')


if __name__ == '__main__':
    main()
