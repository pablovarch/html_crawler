import constants
import psycopg2
import re
from settings import db_connect
from dependencies import log


class Sequence:
    def __init__(self):
        self.__logger = log.Log().get_logger(name=constants.log_file['log_name'])

    def scrape_and_save_sequence(self, list_sequence, ad_event_id):
        self.__logger.info(
            f" --- save sequence url ---")
        try:
            sequence_num = 1
            list_to_save = []
            for seq in list_sequence:
                try:
                    domain = re.findall(r'https?:\/\/([^\/]+)', seq)[0]
                except:
                    pass
                dict_seq = {
                    'sequence_num': sequence_num,
                    'url': seq,
                    'domain': domain,
                    'ad_event_id': ad_event_id
                }
                list_to_save.append(dict_seq)
                sequence_num = sequence_num+1

            for sequence_dict in list_to_save:
                self.save_sequence_url(sequence_dict)
        except Exception as e:
            self.__logger.error('::Sequence:: Error found scrape_and_save_sequence - {}'.format(e))

    def save_sequence_url(self, values_dict):

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
            sql_string = "INSERT INTO public.browser_urls_seq (sequence_num, url, domain, ad_event_id) " \
                         "VALUES(%s, %s, %s, %s) ;"

            data = (values_dict['sequence_num'],
                    values_dict['url'],
                    values_dict['domain'],
                    values_dict['ad_event_id'])
            try:
                # Try to execute the sql_string to save the data
                cursor.execute(sql_string, data)
                conn.commit()
            except Exception as e:
                self.__logger.error('::Sequence:: Error found trying to Save Data sequence - {}'.format(e))

            finally:
                cursor.close()
                conn.close()
    
    def get_sequence_from_domain_id(self, domain_id):
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
            sql_string = """SELECT bus.url 
                            FROM browser_profiles bp 
                            JOIN ad_events ae ON bp.browser_profile_id = ae.browser_profile_id 
                            JOIN browser_urls_seq bus ON bus.ad_event_id = ae.ad_event_id 
                            JOIN domain_attributes da ON da.domain_id = ae.domain_id 
                            WHERE bp.crawler_method ='ForensicPlaywright 2023.08_auto' 
                            AND ae.is_popup = false 
                            AND ae.event_date = (SELECT MAX(event_date) 
                            FROM ad_events 
                            WHERE browser_profile_id = bp.browser_profile_id) 
                            AND da.domain_id = %s 
                            ORDER by bus.sequence_num asc"""

            data = (domain_id,)
            try:
                # Try to execute the sql_string to save the data
                cursor.execute(sql_string, data)
                list_url = cursor.fetchall()
                list_url = [' '.join(item) for item in list_url]
                return list_url
            except Exception as e:
                self.__logger.error('::Sequence:: Error found trying to get sequence - {}'.format(e))

            finally:
                cursor.close()
                conn.close()
        



