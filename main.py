# For all core functionality
from global_variables import *
from global_functions import *
import actions_parsing
import json
import pandas as pd
import numpy as np
import re
import lxml


def parse_weather_data(period_start=pd.Period('2021-10-01'), period_end=pd.Period('2021-10-02'), filename="weather.csv"):
    urls = []
    for i in range((period_end-period_start).n+1):
        period = period_start + i
        url = 'https://www.almanac.com/weather/history/MI/Detroit/{}'.format(period.strftime('%Y-%m-%d'))
        urls.append(url)

    results = []
    actions_parsing.get_requests_html(urls, results)
    selectors = {
        'tag': '.weatherhistory_results'
    }
    tables = [
        {
            'date': re.search('\d{4}-\d{2}-\d{2}', x['initial_url']).group(0),
            'html': str(actions_parsing.get_soup_tags_from_html(x['html'], selectors)['tag'][0])
        } for x in results
    ]
    actions_parsing.save_results_to_file(tables, 'datasets/weather_data_tables.json')
    pass


if __name__ == '__main__':
    parse_weather_data(pd.Period('2010-01-01'), pd.Period('2021-12-31'))