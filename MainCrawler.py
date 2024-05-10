from dependencies import tools, playwright, log, proxy, status_checker
from models import country, domain_attributes, domain_features, subdomain
from datetime import datetime
import constants, settings
import random
import time

class Crawler:
    def __init__(self):
        self.__logger = log.Log().get_logger(name=constants.log_file['log_name'])
        self.__tools = tools.Tools()
        self.__playwright = playwright.Playwright()
        self.__country = country.Country()
        self.__proxy = proxy.Proxy()
        self.__domain_attributes = domain_attributes.Domain_attributes()
        self.__domain_features = domain_features.Domain_features()
        self.__status_checker = status_checker.Status_checker()
        self.__subdomain = subdomain.Subdomain()
        self.__list_country_data = []
        self.__list_country_oxy = []    
        self.__list_all_piracy_kw = []
        self.__list_all_piracy_brands = []
        self.__list_all_consent_kw = []
        self.__list_all_domain_attributes = []

    def crawl(self):
        try:           
            self.__list_country_data = self.__country.get_all_country_data()
            supply_list = self.__tools.read_csv(constants.name_csv_input)
            supply_list = self.__tools.clean_country_supply(supply_list)
            self.__list_country_oxy = self.__tools.read_csv(constants.name_csv_country_oxy)
            self.__list_all_domain_attributes = self.__domain_attributes.get_all_domain_attributes()
            
            self.proxy_service = settings.proxy_service

            self.__logger.info(f" ::MainCrawler:: Crawler started Reading csv Mode  - AdSnifferPlaywright")
            self.__logger.info(f" --- {len(supply_list)} elements")
            for dom in supply_list:
                self.__country_to_scan = dom[1]
                self.__source_domain = dom[0]

                self.subdomains_orchestration()
                         
            self.__logger.info(f" ::MainCrawler:: Crawler ended")

        except Exception as e:
            text = f" ::MainCrawler:: General Exception; {e}"
            self.__logger.error(text)
            
    def subdomains_orchestration(self):       
        # get domain_id
        self.__domain_id = self.__domain_attributes.get_domain_id(self.__list_all_domain_attributes, self.__source_domain)
        # get list of subdomains
        list_subdomains = self.__subdomain.manage_subdomains(self.__domain_id, self.__source_domain, self.__country_to_scan)
        if list_subdomains:
            self.__logger.info(f" --- Scanning {len(list_subdomains)} subdomains for {self.__source_domain} - form {self.__country_to_scan}")
            # for subdom in list_subdomains:
            #     self.proxy_service = settings.proxy_service
            #     self.__subdomain_to_scan = subdom
            #     self.Main_crawler()
            self.proxy_service = settings.proxy_service
            self.__subdomain_to_scan = list_subdomains[0]
            self.Main_crawler()
                
        else:
            self.__logger.info(f" --- No subdomains for {self.__source_domain} - from {self.__country_to_scan}")
            self.__logger.info(f" --- update domain attribites  {self.__source_domain} ")
            status_dict= {
                'online_status': 'Offline | Ad Sniffer',
                'offline_type': 'No subdomains',
                'status_msg': ''
            }
            self.__domain_attributes.manage_update_sites(status_dict, self.__domain_id, self.proxy_service, url='')          
                
    def Main_crawler(self):
        self.__logger.info(f" --- scanning subdomain {self.__subdomain_to_scan}")     
        try:            
            self.__date_ = datetime.now().strftime('%Y-%m-%d')

            # get random profile
            random_profile = random.choice(settings.profile_list)
            
            # get random browser
            random_browser = random.choice(constants.browser_list)
            
            # get country data
            country_data = self.__country.get_country_data_by_country(self.__country_to_scan, self.__list_country_data)

            # get proxy data
            proxy_data = self.__proxy.get_proxy_data(self.__country_to_scan, self.__list_country_data, self.__list_country_oxy, self.proxy_service)

            navigation_flag = self.proxy_orchestration_for_navigation(proxy_data, random_profile, random_browser, country_data)
            if not navigation_flag:
                # try with another proxy -  swap proxy
                 self.__logger.info(f" --- trying with another proxy ---")
                 self.proxy_service = self.__proxy.swap_proxy(self.proxy_service)
                 proxy_data = self.__proxy.get_proxy_data(self.__country_to_scan, self.__list_country_data, self.__list_country_oxy, self.proxy_service)
                 navigation_flag = self.proxy_orchestration_for_navigation(proxy_data, random_profile, random_browser, country_data)
                
                
        except Exception as error:
            self.__logger.error(f'::MainCrawler::Error on Main Crawler {self.__subdomain_to_scan} - {error}')
            # self.save_error_sites(domain_item, coun, error, date_)
            
            
    def proxy_orchestration_for_navigation(self, proxy_data, random_profile,random_browser, country_data):
        try:
            # get domain attributes
            self.__domain_status_dict = self.__domain_attributes.get_domain_status_by_id(self.__domain_id)
            
            # navigation
            self.__logger.info(f" ------ Navigation with  {self.proxy_service} proxy ------")
            dict_domain_source, status_dict, dict_feature_domain = \
                self.__playwright.navigation(self.__subdomain_to_scan,
                                                proxy_data['proxy_dict'],
                                                random_profile,
                                                random_browser['playwright_driver'],
                                                country_data['iso_name'],
                                                self.__domain_status_dict
                                            )
            # update domain_attributes table
            self.__domain_attributes.manage_update_sites(status_dict, self.__domain_id, self.proxy_service, dict_domain_source['url'])

            if status_dict['online_status'] == 'Online':
                # save feature
                try:
                    if dict_feature_domain['html_text']:
                        if self.__domain_id:
                            # num_popups = len(final_list_pop)
                            num_popups = 0
                            dict_feature_domain['num_popups'] = num_popups
                            self.__domain_features.manage_feature(self.__domain_id, self.__date_, dict_feature_domain,
                                                                  self.__subdomain_to_scan)
                except Exception as e:
                    self.__logger.error(f'::MainCrawler::Error on save feature - {e}')
                return True
            else:
                return False
        except Exception as error:
            self.__logger.error(f'::MainCrawler::Error proxy_orchestration  {self.__subdomain_to_scan} - {error}')
            return False          

    def manage_offline_sites(self, status_dict, domain_item, coun, date_):
        try:
            # site arent Online -  update domain
            # self.__logger.info(f'-- site are not online - update domain table - {domain_item}')
            if status_dict['online_status'] == 'Offline':
                status_dict['domain_classification_id'] = 4
            elif status_dict['online_status'] == 'Blocked':
                status_dict['domain_classification_id'] = 2

            self.__domain_attributes.update_domain_attributes(status_dict, domain_item)
            self.__tools.save_csv_name(status_dict, 'review_sites.csv')
            if 'BrightData' in status_dict['offline_type']:
                try:
                    status_dict_brightdata = {
                        'site': domain_item,
                        'country': coun,
                        'status_page': ' Proxy - Blocked',
                        'reason': 'error_tunel',
                        'date': date_
                    }
                    name_csv = f'brightdata_blocked_{date_}'
                    self.__tools.save_csv_name(status_dict_brightdata, name_csv)

                    # re_Scan
                    re_scan_dict = {
                        'site': domain_item,
                        'country': coun,
                    }
                    name_csv_oxy = f're_scan_oxy_{date_}'
                    self.__tools.save_csv_name(re_scan_dict, name_csv_oxy)
                except Exception as er:
                    self.__logger.error(f'cant save brightdata - {er}')
        except Exception as e:
            self.__logger.error(f'-- Error update domain table - {domain_item} -  error: {e}')
        print(status_dict)
