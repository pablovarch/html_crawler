import constants
import psycopg2
from settings import db_connect

from dependencies import log

class Consent_Keywords:
    def __init__(self):
        self.__logger = log.Log().get_logger(name=constants.log_file['log_name'])
    
    def get_all_consent_kw(self):
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
            sql_string = "select language_id , consent_kw from consent_keywords "

            try:
                # Try to execute the sql_string to save the data
                cursor.execute(sql_string)
                respuesta = cursor.fetchall()
                conn.commit()
                if respuesta:
                    list_all_consent_kw = []
                    for elem in respuesta:
                        domain_data = {
                            'language_id': elem[0],
                            'consent_kw': elem[1],

                        }
                        list_all_consent_kw.append(domain_data)
                else:
                    list_all_consent_kw = []

            except Exception as e:
                self.__logger.error('::Consent_Keywords:: Error found trying to get_all_domain_attributes - {}'.format(e))

            finally:
                cursor.close()
                conn.close()
                return list_all_consent_kw
    
    def get_consent_kw_from_language_id(self, list_all_consent_kw, language_id):
        try:
            flag = False
            consent_kw = None

            for elem in list_all_consent_kw:
                if language_id.strip() == elem['language_id']:
                    consent_kw = elem['consent_kw']
                    flag = True
                    break

            return consent_kw, flag

        except Exception as e:
            self.__logger.error('::Consent_Keywords:: Error found trying to get_domain_id - {}'.format(e))
            raise