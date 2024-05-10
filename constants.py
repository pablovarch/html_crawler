name_csv_input = 'supply.csv'
name_csv_country_oxy = 'oxylabs_country.csv'


crawler_version = '2023.08_auto'

log_file = {'log_name': 'Playwright_log 2023.08'}

proxy_dict = {'proxy_residential': 'lum-customer-c_f416ecd9-zone-residential-country-{}:4ar4mlv8uu9i@zproxy.lum-superproxy.io:22225',
              'proxy_mobile': 'brd-customer-hl_f416ecd9-zone-mobile-country-{}:52ggi1lw5ei1@brd.superproxy.io:22225'}

proxy_host = 'lum-customer-c_f416ecd9-zone-residential-country'
proxy_server = 'http://zproxy.lum-superproxy.io:22225'
proxy_password = '4ar4mlv8uu9i'

browser = "MicrosoftEdge"

browser_list = [{'name': 'MicrosoftEdge', 'playwright_driver': 'msedge'},
                {'name': 'GoogleChrome', 'playwright_driver': 'chrome'}]

height = 700