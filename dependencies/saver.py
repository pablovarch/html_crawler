import csv
import os
import requests
from dependencies import log
from datetime import datetime
import constants
import psycopg2
from settings import db_connect


class Saver:

    def __init__(self):
        self.__logger = log.Log().get_logger(name=constants.log_file['log_name'])
        pass

    def save_csv(self, url_dict):
        # open file
        try:
            with open(f"{constants.output_csv['csv_name']}.csv", mode='a', encoding='utf-8') as csv_file:
                headers2 = list(url_dict.keys())
                writer = csv.DictWriter(csv_file, fieldnames=headers2, delimiter=',', lineterminator='\n')
                # create headers
                if os.stat(f"{constants.output_csv['csv_name']}.csv").st_size == 0:
                    writer.writeheader()
                # save data
                writer.writerow(url_dict)
                # self.__logger.info(f"save data for {url_dict['domain']} - {url_dict['javascript_tag']}")
        except Exception as e:
            self.__logger.error(f"cant save data for {url_dict['domain']} - {url_dict['javascript_tag']} - {e}")

    def save_csv_2(self, filename, relation_dict):
        # open file
        try:
            with open(f"{filename}", mode='a', encoding='utf-8') as csv_file:
                headers2 = list(relation_dict.keys())
                writer = csv.DictWriter(csv_file, fieldnames=headers2, delimiter=',', lineterminator='\n')
                # create headers
                if os.stat(f"{filename}.csv").st_size == 0:
                    writer.writeheader()
                # save data
                writer.writerow(relation_dict)
                # self.__logger.info(f"save relation for {relation_dict['domain']}")
        except Exception as e:
            pass
            # self.__logger.error(f"cant save relation for {relation_dict['domain']} - {e}")

    @staticmethod
    def save_csv_3(filename, dict_list):
        # open file
        try:
            head = 0
            with open(f"{filename}.csv", mode='a', encoding='utf-8') as csv_file:
                for indic_dict in dict_list:
                    headers2 = list(indic_dict.keys())
                    writer = csv.DictWriter(csv_file, fieldnames=headers2, delimiter=';', lineterminator='\n')
                    # create headers
                    if head == 0:
                        if os.stat(f"{filename}.csv").st_size == 0:
                            writer.writeheader()
                            head = 1
                    # save data
                    writer.writerow(indic_dict)
        except Exception as e:
            print(f"cant save data for {indic_dict['domain']} - {e}")



    def save_ad_ads(self, values_dict):
        """
        This method try to connect to the DB and save the data
        :param values_dict: dictionary containing the advertiser information
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

            sql_string = "INSERT INTO public.ad_ads(ad_advertiser_url, ad_advertiser_domain, ad_event_id)" \
                         "VALUES(%s,%s,%s);"

            data = (values_dict['ad_advertiser_url'], values_dict['ad_advertiser_domain'], values_dict['ad_event_id'])

            try:
                # Try to execute the sql_string to save the data
                cursor.execute(sql_string, data)
                conn.commit()
            except Exception as e:
                self.__logger.error('::Saver:: Error found trying to Save Data advertiser_urls - {}'.format(e))

            finally:
                cursor.close()
                conn.close()

