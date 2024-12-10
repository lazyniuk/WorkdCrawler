import asyncio
import aiohttp
import json
import time
import random
from collections import defaultdict
from urllib.parse import urlparse
import argparse
import os

URLS_FILE = 'urls.txt'
FILTER_KEYWORDS_FILE = 'filter_keywords.txt'
DAYS_THRESHOLD = 7
MAX_REQUESTS_PER_DOMAIN_PER_MINUTE = 60
MAX_CONCURRENT_REQUESTS = 10

filtered_jobs = []
domains_last_access = defaultdict(float)
domains_request_count = defaultdict(int)

jobs_lock = asyncio.Lock()
domains_lock = asyncio.Lock()

headers = {
    'Accept': 'application/json',
    'Content-Type': 'application/json',
    'Accept-Language': 'en-US,en;q=0.9',
    'Accept-Encoding': 'gzip, deflate, br',
}

base_payload = {
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
    return filter_keywords or set()

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
    if not filter_keywords:
        return False
    fields_to_check = [job.get('title', ''), job.get('location', ''), job.get('jobUrl', '')]
    for field in fields_to_check:
        if any(keyword in field.lower() for keyword in filter_keywords):
            return True
    return False

async def rate_limit_domain(domain):
    """Enforce a per-domain rate limit (MAX_REQUESTS_PER_DOMAIN_PER_MINUTE).
    If the domain is hitting the limit, wait until safe."""
    async with domains_lock:
        now = time.time()
        if domains_request_count[domain] >= MAX_REQUESTS_PER_DOMAIN_PER_MINUTE:
            time_elapsed = now - domains_last_access[domain]
            if time_elapsed < 60:
                await asyncio.sleep(60 - time_elapsed)
                domains_request_count[domain] = 0
        domains_last_access[domain] = time.time()

async def fetch_jobs(session, url, titles):
    base_url = '/'.join(url.split('/')[:3])
    career_site_path = url.split('/')[-1]
    subdomain = base_url.split('//')[1].split('.')[0]
    api_url = f'{base_url}/wday/cxs/{subdomain}/{career_site_path}/jobs'
    referer_url = url
    domain = urlparse(url).netloc

    search_text = " OR ".join(titles)
    payload = dict(base_payload)
    payload['searchText'] = search_text

    await rate_limit_domain(domain)

    try:
        async with session.post(api_url, json=payload, headers={**headers, 'Origin': base_url, 'Referer': referer_url}) as response:
            response.raise_for_status()
            data = await response.json()
            job_postings = data.get('jobPostings', [])
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
                            async with jobs_lock:
                                filtered_jobs.append(filtered_job)
    except aiohttp.ClientError as e:
        print(f"Error fetching jobs from {api_url}: {e}")

    async with domains_lock:
        domains_request_count[domain] += 1
        domains_last_access[domain] = time.time()

    await asyncio.sleep(random.uniform(1, 3))

async def worker(domain_queue, titles, session):
    while True:
        item = await domain_queue.get()
        if item is None:
            domain_queue.task_done()
            break
        domain, url = item
        await fetch_jobs(session, url, titles)
        domain_queue.task_done()

def save_sorted_jobs(output_file):
    sorted_jobs = sorted(filtered_jobs, key=lambda job: parse_posted_date(job['postedOn']))
    with open(output_file, 'w') as f:
        json.dump(sorted_jobs, f, indent=4)
    print(f'Filtered and sorted jobs saved to {output_file}')

async def main_async(titles, output):
    if os.path.exists(titles):
        with open(titles, 'r') as f:
            search_titles = [line.strip() for line in f if line.strip()]
    else:
        search_titles = [title.strip() for title in titles.split(',')]

    if not os.path.exists(URLS_FILE):
        print(f"{URLS_FILE} not found.")
        return

    with open(URLS_FILE, 'r') as f:
        urls = [line.strip() for line in f if line.strip()]

    domain_queue = asyncio.Queue()

    for url in urls:
        domain = urlparse(url).netloc
        await domain_queue.put((domain, url))

    for _ in range(MAX_CONCURRENT_REQUESTS):
        await domain_queue.put(None)

    connector = aiohttp.TCPConnector(limit_per_host=MAX_CONCURRENT_REQUESTS)
    async with aiohttp.ClientSession(connector=connector) as session:
        tasks = [asyncio.create_task(worker(domain_queue, search_titles, session)) for _ in range(MAX_CONCURRENT_REQUESTS)]
        await domain_queue.join()

        for t in tasks:
            await t

    save_sorted_jobs(output)

def main():
    parser = argparse.ArgumentParser(description="Asynchronously crawl Workday job postings.")
    parser.add_argument(
        '-t', '--titles', required=True, help="Comma-separated list of search titles or a file path containing titles."
    )
    parser.add_argument(
        '-o', '--output', required=True, help="Output JSON file for all jobs."
    )
    args = parser.parse_args()

    asyncio.run(main_async(args.titles, args.output))

if __name__ == '__main__':
    main()
