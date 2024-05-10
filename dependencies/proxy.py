import constants, settings
import psycopg2
import random
from dependencies import log
import random


class Proxy:
    def __init__(self):
        self.__logger = log.Log().get_logger(name=constants.log_file['log_name'])

    def get_proxy_data(self, coun, list_country_data, list_country_oxy, proxy_service):

        if proxy_service == 'Brightdata':
            # Brightdata
            proxy_data = self.get_proxy_data_bright_data(list_country_data, coun)
        else:
            # oxylabs
            proxy_data = self.get_proxy_data_oxy(list_country_data, list_country_oxy, coun)

        return proxy_data

    def get_proxy_data_bright_data(self, list_country_data, coun):
        try:
            flag = False
            country_id = None
            proxy = None
            country = None
            if settings.proxy_mobile:
                for elem in list_country_data:
                    if coun.strip() == elem['country']:
                        proxy = constants.proxy_dict['proxy_mobile'].format(elem['iso_name'].lower())
                        country_id = elem['country_id']
                        country = elem['iso_name'].lower()
                        flag = True
                        break      
            else:      
                for elem in list_country_data:
                    if coun.strip() == elem['country']:
                        proxy = constants.proxy_dict['proxy_residential'].format(elem['iso_name'].lower())
                        country_id = elem['country_id']
                        country = elem['iso_name'].lower()
                        flag = True
                        break

            if flag:
                aux = proxy.split("@")
                aux2 = aux[0].split(':')
                server = aux[1]
                username = aux2[0]
                password = aux2[1]

                proxy_dict_playwright = {
                    'server': server,
                    'username': username,
                    'password': password
                }

                if settings.proxy_mobile:
                   proxy_zone= 'mobile'
                else:
                   proxy_zone= 'residential'
                   
                proxy_data = {
                    'proxy_zone': proxy_zone,
                    'country_id': country_id,
                    'country': country,
                    'proxy_dict': proxy_dict_playwright
                }

            return proxy_data
        except Exception as e:
            self.__logger.error(f" ::Get Proxy data Error:: {e}")

    def get_proxy_data_oxy(self, list_country_data, list_country_oxy, coun):
        try:
            country_id = None
            country = None
            proxy_dict_oxylabs = {}
            list_country_found = []

            # get_country_id
            for elem in list_country_data:
                if coun.strip() == elem['country']:
                    country_id = elem['country_id']
                    break
            
            proxy_static = self.check_static_ip_oxy(coun)
            if proxy_static: 
                proxy_dict_oxylabs = {
                            'server': proxy_static,
                            'username': 'contenthound',
                            'password': 'UQ9^yBYr9YRlA2xWwpjQ'
                        }
                proxy_data = {
                    'proxy_zone': 'static',
                    'country_id': country_id,
                    'country': country,
                    'proxy_dict': proxy_dict_oxylabs
                }
                
            else:

                for elem in list_country_oxy:
                    if coun.strip() in elem[0] or elem[0] in coun:
                        list_country_found.append(elem)
                        country = coun

                if list_country_found:
                    if settings.proxy_mobile:
                        
                        random_row = random.choice(list_country_found)
                        # user_name = f'customer-ross_reynolds-cc-{random_row[1]}-city-{random_row[2]}'
                        user_name = f'customer-pablo_varas2-cc-{random_row[1]}'                  
                        proxy_dict_oxylabs = {
                            'server': 'http://pr.oxylabs.io:7777',
                            'username': user_name,
                            'password': 'Madrid912'
                        }                    
                    else:
                        session = random.random()
                        sesstime = 7
                        
                        # residential proxy                          
                        aux_proxy = random.choice(settings.list_users_oxy)
                        random_row = random.choice(list_country_found)                        
                        # aux_proxy = settings.proxy_dict_oxylabs_residential                        
                        user_name = f"{aux_proxy['username']}{random_row[1]}-sessid-{session}-sesstime-{sesstime}"
                        aux_proxy['username'] = user_name
                        proxy_dict_oxylabs = aux_proxy
                        
                if settings.proxy_mobile:
                    proxy_zone= 'mobile'
                else:
                    proxy_zone= 'residential'
                    
                proxy_data = {
                    'proxy_zone': proxy_zone,
                    'country_id': country_id,
                    'country': country,
                    'proxy_dict': proxy_dict_oxylabs
                }

            return proxy_data
        except Exception as e:
            self.__logger.error(f" ::Get Proxy data Error:: {e}")


    def swap_proxy(self, proxy_service):
        if proxy_service == 'Brightdata':
            proxy_swaped = 'Oxylabs'
        else:
            proxy_swaped = 'Brightdata'
        return proxy_swaped   
       

    def check_static_ip_oxy(self, countrie):
        proxy_data = {
            # 'Germany': [
            #     {"ip": "171.22.168.110", "port": "60000", "country": "DE"},
            #     {"ip": "171.22.168.14", "port": "60000", "country": "DE"},
            #     {"ip": "171.22.168.142", "port": "60000", "country": "DE"},
            #     {"ip": "171.22.168.46", "port": "60000", "country": "DE"},
            #     {"ip": "171.22.168.78", "port": "60000", "country": "DE"}
            # ],
            # 'United States': [
            #     {"ip": "23.230.243.106", "port": "60000", "country": "US"},
            #     {"ip": "23.230.243.138", "port": "60000", "country": "US"},
            #     {"ip": "23.230.243.170", "port": "60000", "country": "US"},
            #     {"ip": "23.230.243.202", "port": "60000", "country": "US"},
            #     {"ip": "23.230.243.74", "port": "60000", "country": "US"}
            # ],
            # 'India': [
            #     {"ip": "86.106.24.5", "port": "60000", "country": "IN"},
            #     {"ip": "86.106.24.52", "port": "60000", "country": "IN"},
            #     {"ip": "86.106.24.58", "port": "60000", "country": "IN"},
            #     {"ip": "86.106.24.84", "port": "60000", "country": "IN"},
            #     {"ip": "86.106.24.90", "port": "60000", "country": "IN"}
            # ],
            # 'United Kingdom': [
            #     {"ip": "91.242.249.120", "port": "60000", "country": "GB"},
            #     {"ip": "91.242.249.152", "port": "60000", "country": "GB"},
            #     {"ip": "91.242.249.24", "port": "60000", "country": "GB"},
            #     {"ip": "91.242.249.56", "port": "60000", "country": "GB"},
            #     {"ip": "91.242.249.88", "port": "60000", "country": "GB"}
            # ]
        }

        proxy_static = None

        if countrie in proxy_data:
            proxy_list = proxy_data[countrie]
            proxy = random.choice(proxy_list)
            ip = proxy['ip']
            port = proxy['port']
            proxy_static = f'{ip}:{port}'

        return proxy_static
        