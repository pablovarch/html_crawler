from dependencies import log
import constants
import psycopg2
from settings import db_connect

class Piracy_brands:

    def __init__(self):
        self.__logger = log.Log().get_logger(name=constants.log_file['log_name'])
        
    def get_all_piracy_brands(self):
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
            sql_string = "select piracy_brand from piracy_brands  "

            try:
                # Try to execute the sql_string to save the data
                cursor.execute(sql_string)
                respuesta = cursor.fetchall()
                conn.commit()
                if respuesta:
                    list_all_piracy_brand = []
                    for elem in respuesta:
                        
                        list_all_piracy_brand.append(elem)
                else:
                    list_all_piracy_brand = []

            except Exception as e:
                self.__logger.error('::Saver:: Error found trying to get_all_piracy_brands - {}'.format(e))

            finally:
                cursor.close()
                conn.close()
                return list_all_piracy_brand