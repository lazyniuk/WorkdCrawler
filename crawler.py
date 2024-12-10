import asyncio
import aiohttp
import json
import time
import random
from collections import defaultdict
from urllib.parse import urlparse
import argparse
import os
import logging

URLS_FILE = 'urls.txt'
FILTER_KEYWORDS_FILE = 'filter_keywords.txt'
DAYS_THRESHOLD = 7
MAX_REQUESTS_PER_DOMAIN_PER_MINUTE = 60
MAX_CONCURRENT_REQUESTS = 10

logging.basicConfig(
  level=logging.INFO,
  format='[%(asctime)s] %(levelname)s: %(message)s',
  datefmt='%Y-%m-%d %H:%M:%S'
)

filtered_jobs = []
jobs_lock = asyncio.Lock()
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

class RateLimiter:
  def __init__(self, max_requests_per_minute):
    self.max_requests_per_minute = max_requests_per_minute
    self.domain_request_count = defaultdict(int)
    self.domain_last_access = defaultdict(float)
    self.lock = asyncio.Lock()

  async def throttle(self, domain: str):
    async with self.lock:
      now = time.time()
      if self.domain_request_count[domain] >= self.max_requests_per_minute:
        elapsed = now - self.domain_last_access[domain]
        if elapsed < 60:
          wait_time = 60 - elapsed
          logging.info(f"Rate limit reached for {domain}, waiting {wait_time:.2f}s...")
          await asyncio.sleep(wait_time)
          self.domain_request_count[domain] = 0
      self.domain_last_access[domain] = time.time()

  async def record_request(self, domain: str):
    async with self.lock:
      self.domain_request_count[domain] += 1
      self.domain_last_access[domain] = time.time()

def load_filter_keywords():
  if not os.path.exists(FILTER_KEYWORDS_FILE):
    open(FILTER_KEYWORDS_FILE, 'w').close()
  with open(FILTER_KEYWORDS_FILE, 'r') as f:
    return {line.strip().lower() for line in f if line.strip()} or set()

filter_keywords = load_filter_keywords()

def is_posted_recently(posted_on: str) -> bool:
  posted_on = posted_on.replace('Posted ', '').strip()
  if 'Days Ago' in posted_on:
    try:
      days = int(posted_on.split(' ')[0])
      return days <= DAYS_THRESHOLD
    except ValueError:
      return False
  return 'Today' in posted_on or 'Yesterday' in posted_on

def parse_posted_date(posted_on: str) -> float:
  posted_on = posted_on.replace('Posted ', '').strip()
  if 'Today' in posted_on:
    return 0
  elif 'Yesterday' in posted_on:
    return 1
  elif 'Days Ago' in posted_on:
    try:
      return int(posted_on.split(' ')[0])
    except ValueError:
      return float('inf')
  return float('inf')

def is_job_excluded(job: dict) -> bool:
  if not filter_keywords:
    return False
  fields_to_check = [job.get('title', ''), job.get('location', ''), job.get('jobUrl', '')]
  return any(
    keyword in field.lower()
    for field in fields_to_check
    for keyword in filter_keywords
  )

async def fetch_jobs(session, url: str, search_titles: list, rate_limiter: RateLimiter):
  parsed_url = urlparse(url)
  base_url = f"{parsed_url.scheme}://{parsed_url.netloc}"
  career_site_path = url.split('/')[-1]
  subdomain = parsed_url.netloc.split('.')[0]

  api_url = f'{base_url}/wday/cxs/{subdomain}/{career_site_path}/jobs'
  domain = parsed_url.netloc

  search_text = " OR ".join(search_titles)
  payload = dict(base_payload)
  payload['searchText'] = search_text

  await rate_limiter.throttle(domain)

  try:
    async with session.post(api_url, json=payload, headers={**headers, 'Origin': base_url, 'Referer': url}) as response:
      response.raise_for_status()
      data = await response.json()
      job_postings = data.get('jobPostings', [])

      for job in job_postings:
        if not is_posted_recently(job.get('postedOn', '')):
          continue
        for title in search_titles:
          job_title = job.get('title', '')
          if title.lower() in job_title.lower():
            candidate_job = {
              'title': job_title,
              'location': job.get('locationsText', ''),
              'postedOn': job.get('postedOn', ''),
              'jobUrl': url + job.get('externalPath', ''),
              'searchTitle': title
            }
            if not is_job_excluded(candidate_job):
              async with jobs_lock:
                filtered_jobs.append(candidate_job)
  except aiohttp.ClientError as e:
    logging.warning(f"Error fetching from {api_url}: {e}")

  await rate_limiter.record_request(domain)
  await asyncio.sleep(random.uniform(1, 3))

async def worker(domain_queue: asyncio.Queue, search_titles: list, session: aiohttp.ClientSession, rate_limiter: RateLimiter):
  while True:
    item = await domain_queue.get()
    if item is None:
      domain_queue.task_done()
      break
    _, url = item
    await fetch_jobs(session, url, search_titles, rate_limiter)
    domain_queue.task_done()

def save_sorted_jobs(output_file: str):
  sorted_list = sorted(filtered_jobs, key=lambda j: parse_posted_date(j['postedOn']))
  with open(output_file, 'w') as f:
    json.dump(sorted_list, f, indent=4)
  logging.info(f'Filtered and sorted jobs saved to {output_file}')

async def main_async(titles_input: str, output_file: str):
  if os.path.exists(titles_input):
    with open(titles_input, 'r') as f:
      search_titles = [line.strip() for line in f if line.strip()]
  else:
    search_titles = [t.strip() for t in titles_input.split(',')]

  if not os.path.exists(URLS_FILE):
    logging.error(f"{URLS_FILE} not found.")
    return
  with open(URLS_FILE, 'r') as f:
    urls = [line.strip() for line in f if line.strip()]

  domain_queue = asyncio.Queue()
  for url in urls:
    domain = urlparse(url).netloc
    await domain_queue.put((domain, url))

  for _ in range(MAX_CONCURRENT_REQUESTS):
    await domain_queue.put(None)

  rate_limiter = RateLimiter(MAX_REQUESTS_PER_DOMAIN_PER_MINUTE)
  connector = aiohttp.TCPConnector(limit_per_host=MAX_CONCURRENT_REQUESTS)

  async with aiohttp.ClientSession(connector=connector) as session:
    tasks = [asyncio.create_task(worker(domain_queue, search_titles, session, rate_limiter)) 
             for _ in range(MAX_CONCURRENT_REQUESTS)]
    await domain_queue.join()
    for t in tasks:
      await t

  save_sorted_jobs(output_file)

def main():
  parser = argparse.ArgumentParser(description="Asynchronously crawl Workday job postings with filtering and sorting.")
  parser.add_argument(
    '-t', '--titles', required=True,
    help="Comma-separated list of search titles or a file path containing titles."
  )
  parser.add_argument(
    '-o', '--output', required=True,
    help="Output JSON file for all jobs."
  )
  args = parser.parse_args()

  asyncio.run(main_async(args.titles, args.output))

if __name__ == '__main__':
  main()
