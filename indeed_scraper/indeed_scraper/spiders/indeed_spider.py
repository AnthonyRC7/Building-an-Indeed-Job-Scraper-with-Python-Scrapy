import re
import json
import scrapy
from bs4 import BeautifulSoup
from urllib.parse import urlencode
from scrapy_scrapingbee import ScrapingBeeSpider, ScrapingBeeRequest


class IndeedJobSpider(ScrapingBeeSpider):
    name = "indeed_jobs"

    def get_indeed_search_url(self, keyword, location, offset=0):
        parameters = {"q": keyword, "l": location, "filter": 0, "start": offset}
        return "https://www.indeed.com/jobs?" + urlencode(parameters)

    def start_requests(self):
        keyword_list = ['python']
        location_list = ['texas']
        for keyword in keyword_list:
            for location in location_list:
                indeed_jobs_url = self.get_indeed_search_url(keyword, location)
                yield ScrapingBeeRequest(url=indeed_jobs_url,
                                         params={
                                             'render_js': False,
                                             'premium_proxy': False,
                                         },
                                         callback=self.parse_search_results,
                                         meta={'keyword': keyword, 'location': location, 'offset': 0})

    def parse_search_results(self, response):
        location = response.meta['location']
        keyword = response.meta['keyword']
        offset = response.meta['offset']
        script_tag = re.findall(r'window.mosaic.providerData\["mosaic-provider-jobcards"\]=(\{.+?\});', response.text)
        if script_tag is not None:
            json_blob = json.loads(script_tag[0])

            # Paginate Through Jobs Pages
            if offset == 0:
                meta_data = json_blob["metaData"]["mosaicProviderJobCardsModel"]["tierSummaries"]
                num_results = sum(category["jobCount"] for category in meta_data)
                if num_results > 1000:
                    num_results = 50

                for offset in range(10, num_results + 10, 10):
                    url = self.get_indeed_search_url(keyword, location, offset)
                    yield ScrapingBeeRequest(url=url,
                                             params={
                                                 'render_js': False,
                                                 'premium_proxy': False,
                                             },
                                             callback=self.parse_search_results,
                                             meta={'keyword': keyword, 'location': location, 'offset': offset})

            # Extract Jobs From Search Page
            jobs_list = json_blob['metaData']['mosaicProviderJobCardsModel']['results']
            for index, job in enumerate(jobs_list):
                if job.get('jobkey') is not None:
                    job_url = 'https://www.indeed.com/m/basecamp/viewjob?viewtype=embedded&jk=' + job.get('jobkey')
                    yield ScrapingBeeRequest(url=job_url,
                                             params={
                                                 'render_js': False,
                                                 'premium_proxy': False,
                                             },
                                             callback=self.parse_job,
                                             meta={
                                                 'keyword': keyword,
                                                 'location': location,
                                                 'page': round(offset / 10) + 1 if offset > 0 else 1,
                                                 'position': index,
                                                 'jobKey': job.get('jobkey'),
                                             })

    def parse_job(self, response):
        location = response.meta['location']
        keyword = response.meta['keyword']
        page = response.meta['page']
        position = response.meta['position']
        script_tag = re.findall(r"_initialData=(\{.+?\});", response.text)
        if script_tag is not None:
            json_blob = json.loads(script_tag[0])
            job = json_blob["jobInfoWrapperModel"]["jobInfoModel"]['jobInfoHeaderModel']
            sanitized_job_description = json_blob["jobInfoWrapperModel"]["jobInfoModel"]['sanitizedJobDescription']

            # Clean the HTML markup from the job description using BeautifulSoup
            soup = BeautifulSoup(sanitized_job_description, 'html.parser')
            cleaned_description = soup.get_text(separator='\n').strip()

            yield {
                'keyword': keyword,
                'location': location,
                'page': page,
                'position': position,
                'company': job.get('companyName'),
                'jobkey': response.meta['jobKey'],
                'jobTitle': job.get('jobTitle'),
                'jobDescription': cleaned_description,
            }
            print("**************************GOING GOING GOING GOING***************************")
