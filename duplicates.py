file_path = "./urls.txt"

with open(file_path, "r") as file:
  urls = file.readlines()

unique_urls = sorted(set(url.strip() for url in urls))

with open(file_path, "w") as file:
  file.write("\n".join(unique_urls))

unique_urls
