import constants 
import psycopg2
from datetime import datetime
import pytz
from settings import db_connect
from dependencies import log

class Scan_orchestration:
    def __init__(self):
        self.__logger = log.Log().get_logger(name=constants.log_file['log_name'])        
                
    def get_site_for_timezone(self, timezone):        
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
            sql_string = "select site, proxy_country , time_zone, movie_site  from scan_orchestration mwsa where scanned = false and time_zone = %s"

            dict_response = {}
            try:
                # Try to execute the sql_string to save the data
                cursor.execute(sql_string, (timezone,))
                response_data = cursor.fetchone()
                conn.commit()
                if response_data:                    
                    dict_response = {
                        'site': response_data[0],
                        'proxy_country': response_data[1],
                        'time_zone': response_data[2], 
                        'movie_site': response_data[3]                          
                    }                       

            except Exception as e:
                self.__logger.error('--- Scan Orchestration Error found trying to get weekly domain Data - {}'.format(e))
            finally:
                cursor.close()
                conn.close()
                return dict_response  
            
    def update_weekly_scans(self, site_dict):  
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
            sql_string = "UPDATE scan_orchestration SET scanned=true where site = %s and proxy_country  = %s;"

            dict_response = {}
            try:
                # Try to execute the sql_string to save the data
                cursor.execute(sql_string, (site_dict['site'], site_dict['proxy_country']))
                conn.commit()    
                # self.__logger.info(f' --- Scan Orchestration {site_dict["site"]} - {site_dict["proxy_country"]} Weekly domain Data updated successfully')                                  

            except Exception as e:
                self.__logger.error('Scan Orchestration Error found trying to update site - {}'.format(e))
            finally:
                cursor.close()
                conn.close()
                return dict_response  
        
    
    def get_and_check_site_from_db(self):
        try:            
                # Define the timezones
            timezones = [
                    {'name': 'North America', 'timezone': 'Etc/GMT-6', 'pytz_time_zone': 'US/Pacific'},
                    {'name': 'Africa/Europe', 'timezone': 'Etc/GMT+2','pytz_time_zone': 'Europe/Monaco'},
                    {'name': 'Asia', 'timezone': 'Etc/GMT+8', 'pytz_time_zone': 'Asia/Shanghai'},
                    {'name': 'South America', 'timezone': 'Etc/GMT-4', 'pytz_time_zone': 'America/Argentina/Buenos_Aires'}
                ]
            # Define the time range
            hora_inicio = datetime.strptime('11:59:00', '%H:%M:%S').time()
            hora_fin = datetime.strptime('23:59:00', '%H:%M:%S').time()

            # Iterate over the timezones
            for tz in timezones:
                try: 
                    current_time = datetime.now(pytz.timezone(tz['pytz_time_zone'])).time()
                    if hora_inicio <= current_time <= hora_fin:
                        # self.__logger.info(f"La hora actual en {tz['name']} está dentro del rango de 12 a 00 horas.")      
                        # self.__logger.info(f"Searching site for timezone {tz['timezone']}")                  
                        site_dict = self.get_site_for_timezone(tz['timezone'])
                        if site_dict:
                            return site_dict
                        else:
                            self.__logger.info(f"trying with another timezone")  
                            continue
                except Exception as e:
                    self.__logger.error(f'::Error on get_and_check_site_from_mv - {e}')

            return None   
        except Exception as e:
            self.__logger.error(f'::MainCrawler::Error on get_and_check_site_from_mv - {e}')
            return None