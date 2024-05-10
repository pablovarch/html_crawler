import constants
import json
import psycopg2
from settings import db_connect
from dependencies import log

class Ad_bid:
    def __init__(self):
        self.__logger = log.Log().get_logger(name=constants.log_file['log_name'])

    def set_and_save_bids(self, ad_event_id, list_bids):
        try:
            self.__logger.info(f" --- save bids ---")
            for dict_bid in list_bids:
                try:
                    # set ad_event_id
                    dict_bid['ad_event_id'] = ad_event_id

                    # convert json on string to save
                    resp_json = dict_bid['resp_json']
                    string_json = json.dumps(dict_bid['resp_json'])
                    dict_bid['resp_json'] = string_json

                    list_dict_param = self.flatten_dict(resp_json)
                    ad_bid_id = self.save_ad_bids(dict_bid)
                    # saver.Saver().save_csv_name(dict_bid, 'bid_table.csv')

                    # save parameters
                    list_to_insert_parameters = []
                    for elem in list_dict_param:

                        if elem['value'] != '':
                            data = (elem['value'],
                                    elem['key'],
                                    ad_event_id,
                                    ad_bid_id)
                            list_to_insert_parameters.append(data)
                    self.save_ad_parameter(list_to_insert_parameters)
                except Exception as e:
                    pass
                    # self.__logger.error(f'error save bids {e}')
        except Exception as e:
            self.__logger.error(f'error set_and_save_bids {e}')

    def flatten_dict(self, data):
        try:
            list_dict_param = []
            list_total = []
            for key, value in data.items():
                list_dict_param = self.separarate_dict(key, value, list_total)
            return list_dict_param
        except Exception as e:
            pass
            # self.__logger.error(f'Flatten dict error: {e}')

    def separarate_dict(self, key, value, list_total):
        try:
            if isinstance(value, dict):
                for key, value in value.items():
                    list_total = self.separarate_dict(key, value, list_total)
            elif isinstance(value, list):
                for item in value:
                    list_total = self.separarate_dict(key, item, list_total)
            else:
                dict_param = {'key': key,
                              'value': value}
                list_total.append(dict_param)
            return list_total
        except Exception as e:
            pass
            # self.__logger.error(f'separate dict error: {e}')


    def save_ad_bids(self, values_dict):
        """
        This method try to connect to the DB and save the data
        :param values_dict: dictionary containing the ad_bid information
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

            sql_string = "INSERT INTO public.ad_bids (ad_event_id, site, status, url, ad_domain, ad_subdomain, " \
                         "ad_full_domain, ad_buyer, ad_publisher, resp_json) " \
                         "VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s) RETURNING ad_bid_id ;"

            data = (values_dict['ad_event_id'], values_dict['site'], values_dict['status'], values_dict['url'],
                    values_dict['ad_domain'], values_dict['ad_subdomain'], values_dict['ad_full_domain'],
                    values_dict['ad_buyer'], values_dict['ad_publisher'], values_dict['resp_json'])
            try:
                # Try to execute the sql_string to save the data
                cursor.execute(sql_string, data)
                ad_bid_id = cursor.fetchone()
                conn.commit()
                if ad_bid_id:
                    ad_bid_id = ad_bid_id[0]
                else:
                    ad_bid_id = None
                conn.commit()
            except Exception as e:
                self.__logger.error('::Saver:: Error found trying to Save Data ad_bid - {}'.format(e))

            finally:
                cursor.close()
                conn.close()
                return ad_bid_id


    def save_ad_parameter(self, list_to_insert_parameters):
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

            sql_string = "INSERT INTO public.ad_parameters (parameter, parameter_label, ad_event_id, ad_bid_id)" \
                         " VALUES(%s,%s,%s,%s);"

            # data = (values_dict['parameter'],
            #         values_dict['parameter_label'],
            #         values_dict['ad_event_id'],
            #         values_dict['ad_bid_id'])
            try:
                # Try to execute the sql_string to save the data
                cursor.executemany(sql_string, list_to_insert_parameters)
                conn.commit()
            except Exception as e:
                self.__logger.error('::Saver:: Error found trying to Save Data ad_parameters - {}'.format(e))

            finally:
                cursor.close()
                conn.close()

