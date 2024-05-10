import constants
import psycopg2
from settings import db_connect
import re
from dependencies import log
import random

class Ad_event:
    def __init__(self):
        self.__logger = log.Log().get_logger(name=constants.log_file['log_name'])

    def scrape_and_save_ad_events(self, date_, url, is_popup, domain_id, browser_profile_id, country, session_code,popup_landing_page, online_status):
        try:
            self.__logger.info(
                f" --- scrape and save ad_events ---")
            try:
                domain_aux = re.findall(r'https?:\/\/([^\/]+)', url)[0]
                split_domain = domain_aux.split('.')
                if len(split_domain) > 2:
                     # chek if domain is a ip address
                    if len(split_domain) == 4 and len(split_domain[-4]) == 2 and len(split_domain[-3]) == 2 and len(split_domain[-2]) == 2 and len(split_domain[-1]) == 2:
                        domain = domain_aux 
                    # chek if domain is like  www.google.com.uk
                    elif(len(split_domain[-2]) == 3 or len(split_domain[-2]) == 2) and len(split_domain[-1]) == 2:                     
                        domain = '.'.join(split_domain[-3:])
                        subdomain = '.'.join(split_domain[:-3])
                    else:
                        domain = '.'.join(split_domain[-2:])
                        subdomain = '.'.join(split_domain[:-2])
                else:
                    domain = domain_aux
                    subdomain = None
            except:
                domain = None
                subdomain = None
            dict_ad_events = {
                'event_date': date_,
                'url': url,
                'domain': domain,
                'subdomain':subdomain,
                'is_popup': is_popup,
                'domain_id': domain_id,
                'browser_profile_id': browser_profile_id,
                'country': country,
                'session_code': session_code,
                'popup_landing_page': popup_landing_page,
                'online_status': online_status
            }

            ad_event_id = self.save_ad_event(dict_ad_events)
            return ad_event_id

        except Exception as e:
            self.__logger.error(f'Error on scrape and save ad events - Error {e}')

    def save_ad_event(self, values_dict):
        """
        This method try to connect to the DB and save the data
        :param values_dict: dictionary containing the  ad event information
        """

        # Try to connect to the DB
        try:

            conn = psycopg2.connect(host=db_connect['host'],
                                    database=db_connect['database'],
                                    password=db_connect['password'],
                                    user=db_connect['user'],
                                    port=db_connect['port'])
            cursor = conn.cursor()

        except Exception as e:
            print('::DBConnect:: cant connect to DB Exception: {}'.format(e))
            raise

        else:

            sql_string = "INSERT INTO public.ad_events(event_date, url, domain,subdomain, is_popup, domain_id," \
                         "browser_profile_id,country, session_code, popup_landing_page, online_status)" \
                         " VALUES(%s,%s, %s, %s, %s, %s, %s, %s, %s, %s, %s) RETURNING ad_event_id ;"

            data = (values_dict['event_date'],
                    values_dict['url'],
                    values_dict['domain'],
                    values_dict['subdomain'],
                    values_dict['is_popup'],
                    values_dict['domain_id'],
                    values_dict['browser_profile_id'],
                    values_dict['country'],
                    values_dict['session_code'],
                    values_dict['popup_landing_page'],
                    values_dict['online_status']
                    )

            try:
                # Try to execute the sql_string to save the data
                cursor.execute(sql_string, data)
                ad_event_id = cursor.fetchone()
                conn.commit()
                if ad_event_id:
                    ad_event_id = ad_event_id[0]
                else:
                    ad_event_id = None
            except Exception as e:
                self.__logger.error('::Ad_event:: Error found trying to Save Data ad_event - {}'.format(e))

            finally:
                cursor.close()
                conn.close()
                return ad_event_id
    
    def check_popup_landing_page(self, final_list_pop,  domain_item, list_all_domain_attributes, list_all_piracy_kw, list_all_piracy_brands):
        
        for pop in final_list_pop:
            try:            
                if domain_item in pop['url']:
                    popup_landing_page = True
                elif any(str(pb) in pop['url'] for pb in list_all_piracy_brands):
                    popup_landing_page = True
                elif any(str(kw) in pop['url'] for kw in list_all_piracy_kw):
                    popup_landing_page = True
                elif any(str(da) in pop['url'] for da in list_all_domain_attributes):
                    popup_landing_page = True
                else:
                    popup_landing_page = False
                pop['popup_landing_page'] = popup_landing_page
            except Exception as e:
                self.__logger.error(f'::MainCrawler::Error on check_popup_landing_page - {e}')          
        return final_list_pop

