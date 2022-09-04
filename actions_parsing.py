# Here we will store everything for price parsing
from global_variables import *
from global_functions import *
import asyncio
import aiohttp
import json
from bs4 import BeautifulSoup

async def fetch(url, session):
    """
    This is an instance of single fetch func.
    :param url: url to fetch
    :param session: current session object
    :return: str with page's html code
    """
    print(f'Loading {url}')
    async with session.get(url, ssl=False) as response:
        response_text = await response.text()
        # print(response[:100])
        # Let's keep url for future data processing, so wi will return the dict
        return {
            # Saving initial url we got to let match the result with initial data
            'initial_url': url,
            'url': str(response.url),
            'html': response_text,
            'status': response.status
        }


async def bound_fetch(sem, url, session):
    """
    Func for semaphore usage, to limit connections in parallel
    :param sem: semaphore object
    :param url:
    :param session: current session object
    :return: nothing
    """
    async with sem:
        return await fetch(url, session)


async def run_requests(urls, results):
    """
    Runs GET requests for urls from list, extend results list with page's html code.
    :param urls: list of urls
    :param results: list to gather the results
    :return: True if finished with no errors.
    """
    tasks = []
    sem = asyncio.Semaphore(PARSING_PROCESSES_NUM_IN_PARALLEL)
    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.9; rv:45.0) Gecko/20100101 Firefox/45.0'
    }
    # Creating single session for all connections
    async with aiohttp.ClientSession(headers=headers) as session:
        for url in urls:
            task = asyncio.ensure_future(bound_fetch(sem, url, session))
            tasks.append(task)

        # Making asyncio wait for tasks finished
        responses = await asyncio.gather(*tasks)
        # Adding tasks results to our external container
        results.extend(responses)

    return True


def get_requests_html(urls, results):
    """
    Creates loop and starts requests for urls
    :param urls: list of urls
    :param results: list to collect the results
    :return: True if finished with no errors
    """
    print(f'Starting parsing for {len(urls)} urls')
    # loop = asyncio.get_event_loop()
    # Because this doesn't work in GCP environment we have to create loop manually
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    future = asyncio.ensure_future(run_requests(urls, results))
    loop.run_until_complete(future)
    print('Finished parsing')
    return True


def save_results_to_file(results, file_path = 'datasets/parsed_results.json'):
    """
    This func saves parsed results into a json-formatted file.
    :param results: list of dicts to save in json format
    :param file_path: path to json file (Optional, default is '\datasets\parsed_results.json')
    :return: True if all good
    """
    with open(file_path, 'w') as file:
        json.dump(results, file)
        print(f'Wrote {len(results)} results to file {file_path}')
    return True


def get_soup_tags_from_html(html_str, selectors:dict):
    """
    This func receive html string and dict of CSS selectors, returns dict with the same keys,
    but with list of bs4.element.Tag object found in code by these selectors. If nothing found for the given key
    there will be an empty list.
    :param html_str:
    :param selectors: dict of css-selectors, where the key is a variable name and the value is a selector itself
    (e.g. {'product_id': 'a.prod_id'}
    :return: dict of lists with Tag's objects
    """
    print('Starting to parse data from html')
    # Parsing the data in BS-object
    soup = BeautifulSoup(html_str, 'html.parser')
    # Declare dict to place soups objects
    result_soups = {}
    # Iterating each key to get values
    for key in selectors.keys():
        selector = selectors.get(key)
        result_soups[key] = list(soup.select(selector))
    return result_soups


# if __name__ == '__main__':
#     pass