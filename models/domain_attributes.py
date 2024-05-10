from dependencies import log, tools
import constants
import psycopg2
import re 
from settings import db_connect

class Domain_attributes:

    def __init__(self):
        self.__logger = log.Log().get_logger(name=constants.log_file['log_name'])
        self.__tools = tools.Tools()

    def get_all_domain_attributes(self):
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
            sql_string = "select domain_id , domain from domain_attributes  "

            try:
                # Try to execute the sql_string to save the data
                cursor.execute(sql_string)
                respuesta = cursor.fetchall()
                conn.commit()
                if respuesta:
                    list_all_domain_attributes = []
                    for elem in respuesta:
                        domain_data = {
                            'domain_id': elem[0],
                            'domain': elem[1],

                        }
                        list_all_domain_attributes.append(domain_data)
                else:
                    list_all_domain_attributes = []

            except Exception as e:
                self.__logger.error('::Saver:: Error found trying to get_all_domain_attributes - {}'.format(e))

            finally:
                cursor.close()
                conn.close()
                return list_all_domain_attributes
   
    def get_domain_id(self, list_all_domain_attributes, domain_item):
        
        try:
            flag = False
            domain_id = None

            for elem in list_all_domain_attributes:
                if domain_item.strip() == elem['domain']:
                    domain_id = elem['domain_id']
                    flag = True
                    return domain_id

            if not flag:
                return domain_id

        except Exception as e:
            self.__logger.error(f" ::Get domain id Error:: {e}")

    def update_domain_attributes(self, values_dict, domain_id, url):
        """
        This method try to connect to the DB and save the data
        :param values_dict: dictionary containing the  collection job information
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

            sql_string = "UPDATE public.domain_attributes SET domain_classification_id=%s, online_status=%s, " \
                         "offline_type=%s, site_url=%s, status_msg=%s WHERE domain_id =%s;"

            data = (values_dict['domain_classification_id'],
                    values_dict['online_status'],
                    values_dict['offline_type'],
                    url,
                    values_dict['status_msg'],
                    domain_id
                    )
            try:
                # Try to execute the sql_string to save the data
                cursor.execute(sql_string, data)
                conn.commit()
                self.__logger.info(f"::domain_attributes:: Domain_attributes updated successfully - domain_id {domain_id} - online_status {values_dict['online_status']}")

            except Exception as e:
                self.__logger.error('::Saver:: Error found trying to Update Domain_attributes - {}'.format(e))

            finally:
                cursor.close()
                conn.close()
    
    def manage_update_sites(self, status_dict, domain_id, proxy_service, url) :
        try:
            # site arent Online -  update domain
            # self.__logger.info(f'-- site are not online - update domain table - {domain_id}')
            if status_dict['online_status'] == 'Offline | Ad Sniffer':
                status_dict['domain_classification_id'] = 4
            elif status_dict['online_status'] == 'Blocked':
                status_dict['domain_classification_id'] = 2
                status_dict['offline_type'] = f'{proxy_service} - Blocked'
            elif status_dict['online_status'] == 'Online':
                status_dict['domain_classification_id'] = 2
                # status_dict['offline_type'] = proxy_service
            
            bd_domain_status_dict = self.get_domain_status_by_id(domain_id)
            if status_dict['online_status'] == 'Online' and  bd_domain_status_dict['offline_type'] != 'ScrapingBrowser':
                self.update_domain_attributes(status_dict, domain_id, url)            
            else:
                if bd_domain_status_dict['online_status'] != 'Offline | Analyst':      
                       self.update_domain_attributes(status_dict, domain_id, url) 
                else:
                    self.__logger.info(f'-- The site is already classified for the analyst. - domain_id {domain_id}')           
        except Exception as e:
            self.__logger.error(f'-- Error update domain table - {domain_id} -  error: {e}')

    def save_error_sites(self, domain_item, coun, error, date_):
        try:
            status_dict_brightdata = {
                'site': domain_item,
                'country': coun,
                'status_page': ' Proxy - Blocked',
                'reason': 'error_tunel',
                'error': error
            }
            name_csv = f'error_sites{date_}'
            self.__tools.save_csv_name(status_dict_brightdata, name_csv)

            #re_Scan
            re_scan_dict = {
                'site': domain_item,
                'country': coun,
            }
            name_csv_oxy = f're_scan_oxy_{date_}'
            self.__tools.save_csv_name(re_scan_dict, name_csv_oxy)

        except Exception as er:
            self.__logger.error(f'cant save_error_sites - {er}')

    def get_domain_status_by_id(self, domain_id):
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
            sql_string = "select online_status, offline_type,status_msg from domain_attributes where domain_id = %s"

            try:
                # Try to execute the sql_string to save the data
                cursor.execute(sql_string, (domain_id,))
                respuesta = cursor.fetchone()
                conn.commit()
                if respuesta:
                    dict_status = {
                        'online_status': respuesta[0],
                        'offline_type': respuesta[1],
                        'status_msg': respuesta[2]
                    }                   
                    
                else:
                    dict_status = None

            except Exception as e:
                self.__logger.error('::Saver:: Error found trying to get_online_status_by_id - {}'.format(e))

            finally:
                cursor.close()
                conn.close()
                return dict_status
        
        