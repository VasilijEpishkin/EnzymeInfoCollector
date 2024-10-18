import pandas as pd
from scrapy import Spider, Request
from scrapy_playwright.page import PageMethod
from parsel import Selector
import asyncio
import sys
import redis
import json

# Устанавливаем правильный событийный цикл для Windows
if sys.platform == 'win32':
    asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())

# Подключение к Redis
redis_client = redis.Redis(host='localhost', port=6379, db=0)

class RheaSpider(Spider):
    name = "rhea_spider"

    custom_settings = {
        "PLAYWRIGHT_DEFAULT_NAVIGATION_TIMEOUT": 10000,
        "CONCURRENT_REQUESTS": 32,
    }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Получаем данные из Redis
        uniseq_results_json = redis_client.get('uniseq_results')
        if uniseq_results_json:
            self.data = pd.DataFrame(json.loads(uniseq_results_json))
        else:
            self.data = pd.DataFrame()
        self.data['Text_reaction'] = ""
        self.data['SMILES_reaction'] = ""

    def start_requests(self):
        for index, row in self.data.iterrows():
            entry = row['Entry']
            search_url = f'https://www.rhea-db.org/rhea?query=uniprot%3A{entry}'
            yield Request(
                url=search_url,
                callback=self.parse_search_results,
                meta={
                    "index": index,
                    "row": row,
                    "playwright": True,
                    "playwright_page_methods": [PageMethod("wait_for_selector", 'a[href*="/rhea/"]')],
                },
                errback=self.errback
            )

    async def parse_search_results(self, response):
        index = response.meta["index"]
        row = response.meta["row"]
        links = response.css('a[href*="/rhea/"]::attr(href)').getall()
        reactions = []
        smiles_reactions = []

        for link in links:
            reaction_url = response.urljoin(link)
            request = Request(
                url=reaction_url,
                callback=self.parse_reaction_page,
                meta={
                    "index": index,
                    "playwright": True,
                    "playwright_page_methods": [PageMethod("wait_for_selector", "#equationtext")],
                    "reactions": reactions,
                    "smiles_reactions": smiles_reactions,
                },
                errback=self.errback
            )
            yield request

    async def parse_reaction_page(self, response):
        index = response.meta["index"]
        reactions = response.meta["reactions"]
        smiles_reactions = response.meta["smiles_reactions"]

        selector = Selector(text=response.text)
        text_reaction = selector.css('#equationtext::text').get()

        if text_reaction:
            reactions.append(text_reaction)
            smiles_list = []
            participant_elements = selector.css('.reaction-participants > ul > li.participant')
            for element in participant_elements:
                smiles = element.css("span.cell:contains('SMILES') + span.cell::text").get() or "$"
                smiles_list.append(smiles)

            reactants, _, products = text_reaction.partition('=')
            reactants = reactants.strip().split(' + ')
            products = products.strip().split(' + ')

            if len(reactants) + len(products) == len(smiles_list):
                smiles_reaction = '.'.join(smiles_list[:len(reactants)]) + '>>' + '.'.join(smiles_list[len(reactants):])
                smiles_reactions.append(smiles_reaction)
            else:
                smiles_reactions.append(None)

        # Save results after processing all links
        self.data.at[index, 'Text_reaction'] = "; ".join(reactions)
        self.data.at[index, 'SMILES_reaction'] = "; ".join([smiles for smiles in smiles_reactions if smiles])

    def errback(self, failure):
        self.logger.error(f"Error encountered: {failure}")
        request = failure.request
        index = request.meta.get("index")
        if index is not None:
            self.data.at[index, 'Text_reaction'] = None
            self.data.at[index, 'SMILES_reaction'] = None

    def closed(self, reason):
        # Save data after the spider finishes
        self.data.to_excel('C:/Users/vasae/parsing/Uniprot_seq/Final_data.xlsx', index=False)

# Запуск паука
if __name__ == '__main__':
    from scrapy.crawler import CrawlerProcess
    from scrapy.utils.project import get_project_settings

    process = CrawlerProcess(get_project_settings())
    process.crawl(RheaSpider)
    process.start()