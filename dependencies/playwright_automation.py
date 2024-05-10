import re
from dependencies import log
from settings import screenshot
from models import navigation_screenshot, sequence_url
import constants
import random
import json
import time

class Playwright_automation:
    def __init__(self):
        self.__logger = log.Log().get_logger(name=constants.log_file['log_name'])  
        self.__navigation_screenshot = navigation_screenshot.Navigation_screenshot()
        
        
    def check_home_page(self, page, list_mhtml_traffic):
        html = page.content().lower()
        page.mouse.click(1, 1)
        time.sleep(1) 
        if 'go to home' in html or 'full site'in html or 'href="/home"' in html:
            list_mhtml_traffic = self.go_to_home_page(page, list_mhtml_traffic)
            go_to_home_page = True
        else:
            print('the page is in home page')
            go_to_home_page = False
        return list_mhtml_traffic, go_to_home_page

    def go_to_home_page(self, page, list_mhtml_traffic):
        try:            
            self.__logger.info(f'--try to go to home page --')
            list_a = page.locator("a").all()
            for num ,elem in enumerate(list_a):
                try:
                    href = elem.get_attribute("href")
                    if '/home' in href:
                        if screenshot:
                            # screenshot_ = self.__navigation_screenshot.take_screenshot(page)
                            # dict_screenshot = {
                            #     'name': f'home_page{num}',
                            #     'screenshot': screenshot_}
                            # list_mhtml_traffic.append(dict_screenshot)
                            mhtml = self.__navigation_screenshot.capture_mhtml(page)
                            dict_mhtml = {
                                'name': f'go_to_home_page_mhtml{num}', 
                                'mhtml': mhtml
                            }
                            list_mhtml_traffic.append(dict_mhtml)
                        
                        print(href) 
                        try:  
                            page.mouse.click(1, 1)
                            page.mouse.click(2, 2)
                        except:
                            pass
                        time.sleep(1)                
                        try:
                           
                            page.mouse.click(1, 2)
                            elem.click(timeout=10000)
                            page.wait_for_load_state(timeout=20000)
                            # if screenshot:
                            #     screenshot_ = self.__navigation_screenshot.take_screenshot(page)
                            #     dict_screenshot = {
                            #         'name': f'home_page{num}',
                            #         'screenshot': screenshot_}
                            #     list_screenshots_traffic.append(dict_screenshot)
                            break
                        except:
                            pass
            
                except Exception as e:
                    print(f'error in search /home {e}')  
                    
            return list_mhtml_traffic 

        except Exception as e:
            self.__logger.error(f'Playwright_automation::go_to_home_page -  error {e}')

    def go_to_movie(self, page, list_mhtml_traffic):
        try:
            self.__logger.info(f'--try to go to movie --')
           
            list_movies = self.get_list_element_movie(page)
            if list_movies:
               list_mhtml_traffic =  self.click_with_retry(list_movies, page, list_mhtml_traffic)
            else:
                self.__logger.info(f'--no movies found --')
            return list_mhtml_traffic

        except Exception as e:
            self.__logger.error(f'Playwright_automation::go_to_movie -  error {e}')
            return list_mhtml_traffic

    def click_with_retry(self, list_element, page, list_mhtml_traffic):
        try:
            self.__logger.info(f'--try to click on movie --')
            viewport_size = page.viewport_size
            tries = 3
            trie = 1
            while trie < tries: 
                self.__logger.info(f'--try {trie} --')            
                try:
                    try:
                        page.mouse.click(1, 1)
                        page.mouse.click(2, 2)
                    except: 
                        pass 
                    random_element = random.choice(list_element)
                    if screenshot:
                        mhtml = self.__navigation_screenshot.capture_mhtml(page)
                        dict_mhtml = {
                            'name': f'click_mhtml', 
                            'mhtml': mhtml
                        }
                        list_mhtml_traffic.append(dict_mhtml)                       
                    self.__logger.info(f'--try to click on {random_element} --')
                    try:  
                        random_element.scroll_into_view_if_needed()  
                        random_element.click(timeout=20000)
                        self.__logger.info(f'--waiting load movie --')
                        page.wait_for_load_state(timeout=20000)
                    except:
                        pass 
                                        
                    # if screenshot:
                    #     screenshot_ = self.__navigation_screenshot.take_screenshot(page)
                    #     dict_screenshot = {
                    #         'name': f'movie_page{trie}',
                    #         'screenshot': screenshot_}
                    #     list_screenshots_traffic.append(dict_screenshot)
                    break
                except Exception as er:
                    trie+=1
                    # self.__logger.info(f'error on click movie:{er}')
            return list_mhtml_traffic
                               
        except Exception as e:
            self.__logger.error(f'Error on click_with_retry: {e}')
            
    def get_list_element_movie(self, page):
        href_elem_list = []
        # Utiliza el locator para obtener todas las etiquetas <a>
        anchor_locator = page.locator("a").all()
        current_url = page.url        
        domain_url_page = re.findall(r'https?:\/\/([^\/]+)', current_url)[0]        

        # Itera sobre los elementos y obtén los atributos href
        for anchor in anchor_locator:
            try:
                href = anchor.get_attribute("href")
                if domain_url_page in href or 'watch' in href:
                    href_elem_list.append(anchor)                                   
            except Exception as e:
                pass
        return href_elem_list
    
    # def get_list_elemet_movie(self, page):
    #     self.__logger.info(f'--get list element movie --')
    #     try:
    #         elements = page.get_by_role('link').all()
    #         current_url = page.url        
    #         domain_url_page = re.findall(r'https?:\/\/([^\/]+)', current_url)[0] 
            
    #         for element in elements:
    #             handle = element.element_handle()
    #             text = handle.inner_text().lower()
    #             if domain_url_page in text or 'watch' in text:
    #                 found_flag = True
    #                 try:
    #                     element.click()
    #                     self.__logger.info(f'click successful on {text}')
    #                     break
    #                 except Exception as e:
    #                     self.__logger.error(f'error click on {text} - error {e}')
    #         return list_movies
    #     except Exception as e:
    #         self.__logger.error(f'Playwright_automation::get_list_elemet_movie -  error {e}')
    #         return None
    
        
        

