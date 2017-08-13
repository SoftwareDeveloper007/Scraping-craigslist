from urllib import *
from urllib.parse import urlparse, urljoin
from urllib.request import urlopen
import urllib.request

def download(url, num_retries=5):
    """Download function that also retries 5XX errors"""
    try:
        html = urlopen(url).read()
    except urllib.error.URLError as e:
        print('Download error:', e.reason)
        html = None
        if num_retries > 0:
            if hasattr(e, 'code') and 500 <= e.code < 600:
                # retry 5XX HTTP errors
                html = download_html(url, num_retries-1)
    return html


