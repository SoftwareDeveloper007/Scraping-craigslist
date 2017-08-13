from bs4 import BeautifulSoup
import pymysql
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException
import threading
import time
import csv
from download_html import *

SLEEP_TIME = 2


def correct_data(data):
    while '/' in data:
        data = data.replace('/', '')
    while '(' in data:
        data = data.replace('(', '')
    while ')' in data:
        data = data.replace(')', '')
    return data


def correct_time(data):
    data = data.split('T')
    date = data[0]
    time = data[1].split('-')[0]
    timezone = 'UTC-' + data[1].split('-')[1][0:2] + ":" + data[1].split('-')[1][2:4]

    return [date, time, timezone]


class main_controller():
    def __init__(self):
        zip_codes = [90019, 90006, 90007, 90018, 90016, 90008, 90062, 90037, 90011, 90019, 90043,
                     90047, 90044]
        common_url = 'https://losangeles.craigslist.org/search/apa?search_distance=1&postal={}&availabilityMode=0'
        self.urls = []

        for zip_code in zip_codes:
            self.urls.append({
                'zipcode': zip_code,
                'url': common_url.format(zip_code)
            })
            # print(self.urls)

    def process(self):
        for url in self.urls:
            app = onepage_scraper(url)
            app.total_main_pages_parser()
            app.total_sub_pages_parser()

        print('All done successfully and finally!!!')


class onepage_scraper():
    def __init__(self, url):
        self.zipcode = url['zipcode']
        self.start_url = url['url']
        self.page_cnt = 0
        self.total_links = []
        self.total_links_cnt = 0
        self.base_url = 'https://losangeles.craigslist.org'

        filename = 'result(zipcode={}).csv'.format(self.zipcode)
        self.output_file = open(filename, 'w', encoding='utf-8', newline='')
        self.writer = csv.writer(self.output_file)
        headers = ['Date/time of posting', '$Rent', 'Number of Bedrooms', 'Number of Square Feet', 'Geotag(address)',
                   'Title']
        self.writer.writerow(headers)

    def total_main_pages_parser(self):
        print('Main page is parsing')

        while (self.start_url is not None):
            self.one_main_page_parser()

        print('Parsing main page is done successfully')

        '''
        for link in self.total_links:
            print(link)
        '''

    def one_main_page_parser(self):
        self.page_cnt += 1

        self.html = download(self.start_url)

        print(self.start_url, ': downloading and parsing...')
        # print(self.html)
        soup = BeautifulSoup(self.html, 'html.parser')

        ul = soup.find_all('ul', 'rows')[0]
        li_s = ul.find_all('li', 'result-row')
        print(len(li_s))

        for li in li_s:
            link = li.a['href']
            # if self.base_url in link:
            if 'https' in link:
                self.total_links.append(link)
            else:
                self.total_links.append(self.base_url + link)
            self.total_links_cnt += 1

        buttons = soup.find_all('span', 'buttons')[0]
        next_button = buttons.find_all('a', 'next')[0]
        if self.start_url != self.base_url + next_button['href']:
            self.start_url = self.base_url + next_button['href']
        else:
            self.start_url = None

    def total_sub_pages_parser(self):
        print('Downloading subpages and parsing them...')

        self.threads = []
        self.max_threads = 3

        while self.threads or self.total_links:
            for thread in self.threads:
                if not thread.is_alive():
                    self.threads.remove(thread)

            while len(self.threads) < self.max_threads and self.total_links:
                thread = threading.Thread(target=self.one_sub_page_parser)
                thread.setDaemon(True)
                thread.start()
                self.threads.append(thread)

            time.sleep(SLEEP_TIME)

        self.output_file.close()

    def one_sub_page_parser(self):
        try:
            sub_link = self.total_links.pop()
        except:
            print('total_links are empty')
            sub_link = None

        html = download(sub_link)

        print(sub_link, ': downloading and parsing...')
        soup = BeautifulSoup(html, 'html.parser')
        postingtitle = soup.find_all('span', 'postingtitletext')[0]
        try:
            title = postingtitle.find_all('span', id='titletextonly')[0].text
        except:
            title = ''

        try:
            rent = postingtitle.find_all('span', 'price')[0].text
        except:
            rent = ''

        try:
            br_and_square = postingtitle.find_all('span', 'housing')[0].text
            br = correct_data(br_and_square.split('-')[0])
            square = br_and_square.split('-')[1]
        except:
            br = ''
            square = ''

        try:
            address = correct_data(postingtitle.find_all('small')[0].text)
        except:
            address = ''

        try:
            time = soup.find_all('p', 'postinginfo')[0].find_all('time', 'timeago')[0]['datetime']
            time = correct_time(time)
            time = ' '.join(time)
        except:
            time = ''

        time = time.encode('utf-8')
        rent = rent.encode('utf-8')
        br = br.encode('utf-8')
        square = square.encode('utf-8')
        address = address.encode('utf-8')
        title = title.encode('utf-8')

        row = [time, rent, br, square, address, title]
        self.writer.writerow(row)


if __name__ == '__main__':

    app = main_controller()
    app.process()

