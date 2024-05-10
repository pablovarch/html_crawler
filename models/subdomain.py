import constants
import psycopg2
from settings import db_connect
import re
from dependencies import log
import random
import json
import requests

class Subdomain:
    def __init__(self):
        self.__logger = log.Log().get_logger(name=constants.log_file['log_name'])

    def scrape_and_save_subdomain(self, date_, url, domain_id, country, online_status, offline_type):
        try:
            
            self.__logger.info(
            f" --- scrape and save subdomain ---")          
            try:                
                split_domain = url.split('.')
                if len(split_domain) > 2:
                    # chek if domain is a ip address
                    if len(split_domain) == 4 and len(split_domain[-4]) == 2 and len(split_domain[-3]) == 2 and len(split_domain[-2]) == 2 and len(split_domain[-1]) == 2:
                        domain = url 
                    # chek if domain is like  www.google.com.uk
                    elif(len(split_domain[-2]) == 3 or len(split_domain[-2]) == 2) and len(split_domain[-1]) == 2:                     
                        domain = '.'.join(split_domain[-3:])
                        subdomain = '.'.join(split_domain[:-3])
                    else:
                        domain = '.'.join(split_domain[-2:])
                        subdomain = '.'.join(split_domain[:-2])
                else:
                    domain = url
                    subdomain = None
            except:
                domain = None
                subdomain = None
            dict_subdomain = {                               
                'subdomain':subdomain,
                'domain': domain,
                'full_url': url,
                'country': country,
                'date_sourced': date_,
                'online_status': online_status,
                'offline_type': offline_type,                
                'domain_id': str(domain_id),           
            }
            subdomain_check = self.check_subdomain(url, country)
            if not subdomain_check:  
                self.save_subdomain(dict_subdomain)
            else:
                self.__logger.info(
                f" --- subdomain already exists --- update")
                self.update_subdomain(dict_subdomain)
                
                
        except Exception as e:
            self.__logger.error(f'Error on scrape and save subdomain - Error {e}')
            
    def update_subdomain(self, values_dict):
        
        """
        This method try to connect to the DB and save the data
        :param values_dict: dictionary containing the subdomain information
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

            sql_string = """UPDATE public.subdomains SET online_status=%s, offline_type=%s 
                            WHERE domain_id=%s and subdomain=%s and country=%s;"""
                            
                           

            data = (
                    values_dict['online_status'],
                    values_dict['offline_type'],
                    values_dict['domain_id'],
                    values_dict['subdomain'],  
                    values_dict['country'], 
                    )

            try:
                # Try to execute the sql_string to save the data
                cursor.execute(sql_string, data)
                conn.commit()
            except Exception as e:
                self.__logger.error('::subdomain:: Error found trying to Save Data subdomain - {}'.format(e))

            finally:
                cursor.close()
                conn.close()
        

    def save_subdomain(self, values_dict):
        """
        This method try to connect to the DB and save the data
        :param values_dict: dictionary containing the subdomain information
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

            sql_string = """INSERT INTO public.subdomains(subdomain, domain, full_url, country ,date_sourced, online_status, offline_type, domain_id)
                            VALUES(%s,%s, %s, %s, %s, %s, %s, %s);"""

            data = (values_dict['subdomain'],
                    values_dict['domain'],  
                    values_dict['full_url'],
                    values_dict['country'], 
                    values_dict['date_sourced'],
                    values_dict['online_status'],
                    values_dict['offline_type'],
                    values_dict['domain_id'],
                    )

            try:
                # Try to execute the sql_string to save the data
                cursor.execute(sql_string, data)
                conn.commit()
            except Exception as e:
                self.__logger.error('::subdomain:: Error found trying to Save Data subdomain - {}'.format(e))

            finally:
                cursor.close()
                conn.close()
                
    def check_subdomain(self, url, country):
        """
        This method try to connect to the DB and save the data
        :param values_dict: dictionary containing the subdomain information
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

            sql_string = """SELECT subdomain_id FROM public.subdomains WHERE full_url = %s and country = %s;"""

            data = (url,country)   

            try:
                # Try to execute the sql_string to save the data
                cursor.execute(sql_string, data)
                subdomain = cursor.fetchone()
                conn.commit()
                if subdomain:
                    subdomain_check =  True
                else:
                    subdomain_check = False
            except Exception as e:
                self.__logger.error('::subdomain:: Error found trying to Save Data subdomain - {}'.format(e))

            finally:
                cursor.close()
                conn.close()
                return  subdomain_check
    
    def get_subdomains_oxy_api(self, domain_source, country):
        try:
            list_dom = []
            
            url = "https://realtime.oxylabs.io/v1/queries"
            query = f' site:{domain_source}'
            payload = json.dumps({
            "source": "google_search",
            "domain": "com",
            "query": query,
            "geo_location": country,
            "parse": True
            })
            headers = {
            'Content-Type': 'application/json',
            'Authorization': 'Basic ZGF0YV9zY2llbmNlOm5xRHJ4UUZMeHNxNUpOOHpTekdwMg=='
            }

            response = requests.request("POST", url, headers=headers, data=payload)
            json_response = json.loads(response.text)
            json_result = json_response['results'][0]['content']['results']['organic']

            
            for elem in json_result:
                try:                    
                    domain = re.findall(r'https?:\/\/([^\/]+)', elem['url'])[0]
                    if domain_source in domain:
                        dict_subdomain = {
                            'url': elem['url'],
                            'domain': domain
                        }
                        list_dom.append(dict_subdomain)                           
                except:
                    print(f'error compare domains {elem}')

            # delete duplicates list_dom
            list_dom = self.delete_duplicates_subdomains(list_dom)

        except Exception as e:
            self.__logger.error(f" ::Get subdomains Error:: {e}")        
        return list_dom
            
    def get_subdomains(self, domain_id, country):
        """
        This method try to connect to the DB and save the data
        :param values_dict: dictionary containing the subdomain information
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

            sql_string = """SELECT full_url FROM public.subdomains WHERE domain_id = %s and country = %s and online_status='Online';"""
            domain_id_ = str(domain_id)
            data = (domain_id_,country)   

            try:
                # Try to execute the sql_string to save the data
                cursor.execute(sql_string, data)
                subdomains = cursor.fetchall()
                conn.commit()
                if subdomains:
                    subdomains = [item[0] for item in subdomains]
                else:
                    subdomains = []
            except Exception as e:
                self.__logger.error('::subdomain:: Error found trying to Save Data subdomain - {}'.format(e))

            finally:
                cursor.close()
                conn.close()
                return subdomains
            
    def manage_subdomains(self, domain_id, domain_source, country):
        try:
            subdomains = self.get_subdomains(domain_id, country)
            if not subdomains:                
                subdomains = self.get_subdomains_oxy_api(domain_source, country)                                            
        except Exception as e:
            self.__logger.error(f'Error on manage subdomains - Error {e}')
        return subdomains
    
    def delete_duplicates_subdomains(self, subdomains):
        # Conjunto para llevar un registro de dominios únicos
        dominios_vistos = set()

        # Lista para almacenar elementos únicos basados en el dominio
        lista_sin_repetidos = []
        try:       
            for elemento in subdomains:
                # Verifica si el dominio ya ha sido visto
                if elemento['domain'] not in dominios_vistos:
                    # Agrega el dominio al conjunto de dominios vistos
                    dominios_vistos.add(elemento['domain'])
                    # Agrega el elemento a la lista de elementos únicos
                    lista_sin_repetidos.append(elemento['domain'])
                    
            return lista_sin_repetidos
        except Exception as e:
            self.__logger.error(f'Error on delete duplicates subdomains - Error {e}')
            return lista_sin_repetidos