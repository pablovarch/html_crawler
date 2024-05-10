import constants
import psycopg2
from settings import db_connect
import re
from datetime import datetime
from dependencies import  log

class Ad_chain:
    def __init__(self):
        self.__logger = log.Log().get_logger(name=constants.log_file['log_name'])

    def scrape_and_save_ad_chains_url(self, date_, url_page, reqs, ad_event_id, domain_item):
        try:
            self.__logger.info(
                f" --- scrape and save ad_chains_url ---")
            list_dict_ad_chain = []
            collection_timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')
            collection_timestamp = collection_timestamp[:-7]
            try:
                domain_url_page = re.findall(r'https?:\/\/([^\/]+)', url_page)[0]
            except Exception as e:
                self.__logger.error(
                    f" --- cant get domain_url_page --- on {url_page} -  error {e}")
                pass

            ad_url_num = 0
            for req in reqs:
                if domain_url_page in req['url']:
                    continue
                else:
                    try:
                        if 'www.google.com/cse/static' in req['url']:
                            domain = 'www.google.com/cse/static'
                        elif 'www.google.com/recaptcha/' in req['url']:
                            domain = 'www.google.com/recaptcha'
                        else:
                            domain_aux = re.findall(r'https?:\/\/([^\/]+)', req['url'])[0]
                            split_domain = domain_aux.split('.')
                            if len(split_domain) > 2:
                                # chek if domain is a ip address
                                if len(split_domain) == 4 and len(split_domain[-4]) == 2 and len(split_domain[-3]) == 2 and len(split_domain[-2]) == 2 and len(split_domain[-1]) == 2:
                                    domain = domain_aux 
                                # chek if domain is like  www.google.com.uk
                                elif (len(split_domain[-2]) == 3 or len(split_domain[-2]) == 2) and len(split_domain[-1]) == 2:                         
                                    domain = '.'.join(split_domain[-3:])
                                # chek if domain is like  www.google.com
                                else:
                                    domain = '.'.join(split_domain[-2:])
                            else:
                                domain = domain_aux
                    except:
                        domain = None
                        
                    
                    domain = self.check_domain_url(domain, req['url'])                

                    dict_ad_chain_urls = {
                        'ad_url_domain': domain,
                        'ad_url': req['url'],
                        'ad_url_num': ad_url_num,
                        'ad_event_id': ad_event_id,
                        'collection_timestamp': collection_timestamp,
                        'source_domain': domain_item,
                        'collection_date': date_
                    }
                    list_dict_ad_chain.append(dict_ad_chain_urls)
                    ad_url_num += 1

            self.set_to_save_ad_chain(list_dict_ad_chain)
        except Exception as e:
            self.__logger.error(
                f" --- cant get ad_chains_url --- error {e}")

    def set_to_save_ad_chain(self, list_ad_chain):

        if list_ad_chain:
            list_to_insert = []
            for dict_ad_chain_urls in list_ad_chain:

                # saver.Saver().save_csv_name(dict_ad_chain_urls, 'result_ad_company.csv')
                data = (
                    dict_ad_chain_urls['ad_url_num'],
                    dict_ad_chain_urls['ad_url'],
                    dict_ad_chain_urls['ad_url_domain'],
                    dict_ad_chain_urls['ad_event_id'],
                    dict_ad_chain_urls['collection_timestamp'],
                    dict_ad_chain_urls['source_domain'],
                    dict_ad_chain_urls['collection_date']
                )
                list_to_insert.append(data)
            self.save_ad_chain_urls(list_to_insert)

    def save_ad_chain_urls(self, list_to_insert):
        """
        This method try to connect to the DB and save the data
        :param values_dict: dictionary containing the ad_chain information
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
            sql_string = "INSERT INTO ad_chain_urls( ad_url_num, ad_url, ad_url_domain, ad_event_id," \
                         "collection_timestamp, source_domain,collection_date) " \
                         "VALUES(%s,%s,%s,%s,%s,%s,%s);"

            try:
                # Try to execute the sql_string to save the data
                cursor.executemany(sql_string, list_to_insert)
                conn.commit()
            except Exception as e:
                self.__logger.error('::Saver:: Error found trying to Save Data ad_chain_urls - {}'.format(e))

            finally:
                cursor.close()
                conn.close()

    
    def check_domain_url(self,domain_input, url):
        try:
            if 'lambda-url.us-west-2.on.aws' in url:
                domain = 'lambda-url.us-west-2.on.aws'
            elif 'safeframe.googlesyndication.com' in url:
                domain = 'safeframe.googlesyndication.com'
            elif 'metric.gstatic.com' in url:            
                domain = 'metric.gstatic.com'
            elif 'akamaihd.net' in url:
                domain = 'akamaihd.net'
            elif 'amazonaws.com' in url:
                domain = 'amazonaws.com'
            elif 'publisher-services.amazon.dev' in url:   
                domain = 'publisher-services.amazon.dev'
            elif 'cloudfront.aws.a2z.com' in url:
                domain = 'cloudfront.aws.a2z.com'
            elif 'c.2mdn.net' in url:
                domain = 'c.2mdn.net'
            else:
                domain = domain_input

        except:
            domain = domain_input
        return domain
