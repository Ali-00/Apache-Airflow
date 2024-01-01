import requests
import csv
import argparse
import logging
import pandas as pd
import google
from google.cloud import storage


repository_count = 30
max_commit_count = 30

def get_repo_data(url):
    """Get repository details from a given url"""
    try:
        response = requests.get(url)
        if response.status_code == 200:
            return response.json()
        else:
            logging.error(f"Error {response.status_code} getting data from {url}")
            return None
    except requests.exceptions.RequestException as e:
        logging.error(f"Request exception while getting data from {url}: {str(e)}")
        return None


def extract_repo_details(repo_data):
    repo_details = []
    for repo in repo_data:
        if len(repo_details) == repository_count:
            break
        try:
            repo_id = repo['id']
            repo_name = repo['name']
            repo_url = repo['html_url']
            repo_fullname = repo['full_name']
            is_private = repo['private']
            repo_description = repo['description']
            owner_username = repo['owner']['login']
            owner_url = repo['owner']['html_url']
            owner_type = repo['owner']['type']
            owner_site_admin = repo['owner']['site_admin']
            is_fork = repo['fork']
            repo_details.append((repo_id, repo_name, repo_url, repo_fullname, is_private, repo_description,
                                owner_username, owner_url, owner_type, owner_site_admin, is_fork))
        except requests.exceptions.RequestException as e:
            logging.error(f"Request exception while getting data")
            return None
    return repo_details


def extract_commit_details(repo_data):
    commit_data = []
    for repo in repo_data:
        commit_url = f"{repo['url']}/commits"
        try:
            response = requests.get(commit_url)
            if response.status_code == 200:
                commits = response.json()[:max_commit_count]
                for commit in commits:
                    commit_data.append([
                        commit['sha'],
                        commit['commit']['message'],
                        commit['html_url'],
                        commit['commit']['author']['name'],
                        commit['author']['login'],
                        commit['commit']['author']['email'],
                        commit['author']['html_url'],
                        commit['commit']['committer']['name'],
                        commit['committer']['login'],
                        commit['commit']['committer']['email'],
                        commit['committer']['html_url'],
                    ])
            else:
                logging.error(f"Error {response.status_code}")
                return None
            return commit_data
        except requests.exceptions.RequestException as e:
            logging.error(f"Request exception while getting data")
            return None
            

if __name__ == "__main__":
    url = "https://api.github.com/repositories?since=10000"
    repo_data = get_repo_data(url)
    repo_details = extract_repo_details(repo_data)
    commit_data = extract_commit_details(repo_data)

    df = pd.DataFrame(repo_details, columns=['repo_id', 'repo_name', 'repo_url','repo_fullname','is_private','repo_description','owner_username','owner_url','owner_type','owner_site_admin','is_fork'])
    df1 = pd.DataFrame(commit_data, columns=['commit_hash', 'commit_message', 'commit_url','author_name','author_username','author_email','author_url','committer_name','committere_username','committer_email','committer_url'])


    client = storage.Client()
    export_bucket = client.get_bucket('github_crawler_muzamal')

    export_bucket.blob('repo.csv').upload_from_string(df.to_csv(),'text/csv')
    export_bucket.blob('commit.csv').upload_from_string(df.to_csv(),'text/csv')