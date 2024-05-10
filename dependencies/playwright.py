import re
from dependencies import log, Playwright_traffic
from models import navigation_screenshot
import time
import constants, settings
import json
import io
import asyncio  
from playwright.async_api import async_playwright  

from playwright.sync_api import Playwright, sync_playwright, BrowserType


class Playwright:
    def __init__(self):
        self.__logger = log.Log().get_logger(name=constants.log_file['log_name'])
        self.__navigation_screenshot = navigation_screenshot.Navigation_screenshot()

    def navigation(self, domain_item, proxy_dict_playwright, random_profile, random_browser, iso_name, domain_status_dict):
        try:
            # scraping mode orchestration
            if domain_status_dict['online_status'] == 'Blocked' or domain_status_dict['offline_type'] == 'ScrapingBrowser':
                dict_domain_source, status_dict, dict_feature_domain = self.scraping_browser(domain_item)
                if status_dict['online_status']=='Blocked':
                    status_dict['online_status']='Online'
                    status_dict['offline_type']='ScrapingBrowser'
            else:
                dict_domain_source, status_dict, dict_feature_domain, = self.playwrigt_local(domain_item, random_browser, iso_name, proxy_dict_playwright, random_profile)
              
            return dict_domain_source, status_dict, dict_feature_domain
                
            
        except Exception as e:
            # self.__logger.error(f'error on Navigation - create driver - error {e}')
            raise        

    def scraping_browser(self, domain_item):
        try:
           self.__logger.info(
                f" --- set playwright chromium with ScrapingBrowser ---")
           with sync_playwright() as pw: 
                AUTH = 'brd-customer-hl_f416ecd9-zone-bd_scraping_browser_1:tqa4jbuibqcp'  
                SBR_WS_CDP = f'wss://{AUTH}@brd.superproxy.io:9222'  
                
                print('Connecting to Scraping Browser...')  
                browser = pw.chromium.connect_over_cdp(SBR_WS_CDP)  
        
                print('Connected! Navigating...')  
                page = browser.new_page()

                dict_domain_source, status_dict, dict_feature_domain = Playwright_traffic.Playwright_traffic().capture_traffic(page, domain_item)
                                 
                # Cierra el navegador y el contexto                    
                page.close()
                browser.close()
                return dict_domain_source, status_dict, dict_feature_domain
        except Exception as e:
            self.__logger.error(f'error on scraping_browser - create driver - error {e}')
            raise
    
    def playwrigt_local(self, domain_item, random_browser, iso_name, proxy_dict_playwright, random_profile):
        self.__logger.info(
                    f" --- set playwright chromium with local playwright ---")   
        try:            
             # Inicializa Playwright y crea un contexto
            with sync_playwright() as p:
                pw: Playwright = p
                browser = pw.chromium.launch(channel=random_browser, headless=False)
                proxy_dict = None
                if settings.proxy:
                        proxy_dict=proxy_dict_playwright
                        
                if not iso_name:
                    iso_name = 'en-US'
                    
                context = p.chromium.launch_persistent_context(


                    user_data_dir=settings.path_user_data_dir,
                    headless=False,

                    channel=random_browser,

                    args=[
                        # f"--disable-extensions-except={constants.path_to_extension}",
                        # f"--load-extension={constants.path_to_extension}",
                        # f'--profile-directory={constants.user_profile}'
                        f'--profile-directory={random_profile}',
                        "--start-fullscreen"
                    ],
                    
                    proxy = proxy_dict,
                    locale=f'{iso_name}'

                )
                page = context.new_page()

                # Captura el tráfico de red y procesa las solicitudes HTTP

                dict_domain_source, status_dict, dict_feature_domain = Playwright_traffic.Playwright_traffic().capture_traffic(page, domain_item)
                                    
                # Cierra el navegador y el contexto                    
                page.close()
                context.close()
                browser.close()
                return dict_domain_source, status_dict, dict_feature_domain
        except Exception as e:
            self.__logger.error(f'error on playwrigt_local - create driver - error {e}')
            raise