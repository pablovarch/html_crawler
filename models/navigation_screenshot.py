import constants
import settings
import psycopg2
import re
from PIL import Image
import boto3
import io
from io import BytesIO
from settings import db_connect,db_connect_app, dict_aws_private, dict_aws_public
from dependencies import log


class Navigation_screenshot:
    def __init__(self):
        self.__logger = log.Log().get_logger(name=constants.log_file['log_name'])
        
    def save_list_screenshots(self, list_screenshots, ad_event_id):
        self.__logger.info(f" --- save navigation Screenshots ---")
        for screenshot in list_screenshots:
            self.save_screenshot(screenshot, ad_event_id) 
        
    def save_screenshot(self, values_dict, ad_event_id):

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
            sql_string = "INSERT INTO public.navigation_screenshots (screenshot_name, screenshot_file,ad_event_id) " \
                         "VALUES(%s, %s, %s) ;"

            data = (
                    values_dict['name'],
                    psycopg2.Binary(values_dict['screenshot']),
                    ad_event_id)
            try:
                # Try to execute the sql_string to save the data
                cursor.execute(sql_string, data)
                conn.commit()
            except Exception as e:
                self.__logger.error('::Sequence:: Error found trying to Save Data Navigation_screenshots - {}'.format(e))

            finally:
                cursor.close()
                conn.close()
                           
    def capture_screenshot(self, page):
        list_screenshots = []
        all_tab = page.context.pages
        del all_tab[0]
        screenshot = self.take_screenshot(all_tab[0])
        dict_screenshot = {
            'name': 'main_page',
            'screenshot': screenshot}
        list_screenshots.append(dict_screenshot)
        del all_tab[0] 
        # # get all tab screenshots
        # for idx, tab in enumerate(all_tab):
        #     try: 
        #         screenshot = self.take_screenshot(tab) 
        #         name = f'tab_{idx}'
        #         dict_screenshot = {                                    
        #             'name': name,
        #             'screenshot': screenshot}
        #         list_screenshots.append(dict_screenshot)                          
                
        #     except Exception as e:
        #         print(f'error on screenshot {e}') 
        return list_screenshots
                           
    def take_screenshot(self, page_):
        # Capturar la imagen en formato bytes
        screenshot_bytes = page_.screenshot(full_page=True)
        # Convierte la imagen bytes a imagen Pillow
        mem_file = BytesIO()
        mem_file.write(screenshot_bytes)
        img = Image.open(mem_file)
        
        # Resize image
        width, height = img.size
        ratio = width / height
        new_height = constants.height
        new_width = int(ratio * new_height)
        img = img.resize((new_width, new_height))  
        # Convertir la imagen Pillow a bytes
        salida = BytesIO()
        img.save(salida, format='png')
        screenshot_output = salida.getvalue()
        img.close()
        return screenshot_output
       
    def capture_mhtml(self, page):
        client = page.context.new_cdp_session(page)
        mhtml = client.send("Page.captureSnapshot")['data']
        path_ = 'example.mhtml'
        with open(path_, mode='w', encoding='UTF-8', newline='\n') as file:
            file.write(mhtml)
        # mhtml = page.content()
        # return mhtml
         
    def get_screenshot(self):
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
            sql_string = "SELECT screenshot_file FROM public.navigation_screenshots WHERE name = 'main_page'and  ad_event_id = 649 ;"

            try:
                # Try to execute the sql_string to save the data
                cursor.execute(sql_string)
                respuesta = cursor.fetchall()
                conn.commit()
                if respuesta:
                    for elem in respuesta:
                        screenshot = elem[0]
                        screenshot = Image.open(io.BytesIO(screenshot))
                        screenshot.save('./screenshots/example.png')
            except Exception as e:
                self.__logger.error('::Sequence:: Error found trying to get Data Navigation_screenshots - {}'.format(e))

            finally:
                cursor.close()
                conn.close()
                
    def save_list_screenshots_on_file(self, list_Screenshot, ad_event_id):
        self.__logger.info(f" --- save navigation Screenshots  files ---")
        for i, elem in enumerate(list_Screenshot):
            try:
                name = f'{ad_event_id}_{i}_{elem["name"]}'
                screenshot = elem['screenshot']
                screenshot = Image.open(io.BytesIO(screenshot))
                screenshot.save(f'./screenshots/{name}.png')
            except Exception as e:
                print(f'error on screenshot {e}')
                
    def save_list_screenshots_on_s3(self, list_Screenshot, ad_event_id, domain_item):
        self.__logger.info(f" --- save navigation Screenshots  files ---")
        
        # Crea instancias de cliente de S3
        s3_private = boto3.client('s3',
                                  aws_access_key_id= dict_aws_private['aws_access_key_id'],
                                  aws_secret_access_key=dict_aws_private['aws_secret_access_key'],
                                  region_name=dict_aws_private['region_name'])
        
        s3_public = boto3.client('s3',
                                 aws_access_key_id= dict_aws_public['aws_access_key_id'],
                                 aws_secret_access_key=dict_aws_public['aws_secret_access_key'],
                                 region_name=dict_aws_public['region_name'])
        list_to_insert=[]
        for i, elem in enumerate(list_Screenshot):
            try:
                nombre_objeto_s3 = f'{ad_event_id}/{ad_event_id}_{domain_item}_{i}_{elem["name"]}.png'
                screenshot = elem['screenshot']            

                # Sube la captura de pantalla a S3 privado
                s3_private.upload_fileobj(io.BytesIO(screenshot), dict_aws_private['bucket_name'], nombre_objeto_s3)
                print(f'Se subió la captura de pantalla {nombre_objeto_s3} en {dict_aws_private["bucket_name"]}.')
                
                # sube la captura de pantalla a S3 publico
                s3_public.upload_fileobj(io.BytesIO(screenshot), dict_aws_public['bucket_name'], nombre_objeto_s3)
                print(f'Se subió la captura de pantalla {nombre_objeto_s3} en {dict_aws_public["bucket_name"]}.')
                data = (
                    ad_event_id,
                    nombre_objeto_s3,
                )
                list_to_insert.append(data)   
            except Exception as e:
                print(f'error on screenshot {e}')
        #save on db_private        
        self.save_url_screenshot_s3(list_to_insert, db_connect)  
        #save on db_public
        self.save_url_screenshot_s3(list_to_insert, db_connect_app)
        
    def save_list_screenshots_pop_on_s3(self, dict_screenshot, ad_event_id, domain_item):
        self.__logger.info(f" --- save navigation Screenshots  files ---")
        
        # Crea instancias de cliente de S3
        s3_private = boto3.client('s3',
                                  aws_access_key_id= dict_aws_private['aws_access_key_id'],
                                  aws_secret_access_key=dict_aws_private['aws_secret_access_key'],
                                  region_name=dict_aws_private['region_name'])
        
        s3_public = boto3.client('s3',
                                 aws_access_key_id= dict_aws_public['aws_access_key_id'],
                                 aws_secret_access_key=dict_aws_public['aws_secret_access_key'],
                                 region_name=dict_aws_public['region_name'])
        list_to_insert=[]
        
        try:
            nombre_objeto_s3 = f'{ad_event_id}/{ad_event_id}_{domain_item}_{dict_screenshot["name"]}.png'
            screenshot = dict_screenshot['screenshot']            

            # Sube la captura de pantalla a S3 privado
            s3_private.upload_fileobj(io.BytesIO(screenshot), dict_aws_private['bucket_name'], nombre_objeto_s3)
            print(f'Se subió la captura de pantalla {nombre_objeto_s3} en {dict_aws_private["bucket_name"]}.')
            
            # sube la captura de pantalla a S3 publico
            s3_public.upload_fileobj(io.BytesIO(screenshot), dict_aws_public['bucket_name'], nombre_objeto_s3)
            print(f'Se subió la captura de pantalla {nombre_objeto_s3} en {dict_aws_public["bucket_name"]}.')
            data = (
                ad_event_id,
                nombre_objeto_s3,
            )
            list_to_insert.append(data)   
        except Exception as e:
            print(f'error on save_list_screenshots_pop_on_s3 {e}')
        #save on db_private        
        self.save_url_screenshot_s3(list_to_insert, db_connect)  
        #save on db_public
        self.save_url_screenshot_s3(list_to_insert, db_connect_app) 
                     
    def get_screenshot_from_s3(self):

        # Ruta del objeto en S3 (incluyendo el nombre del archivo)
        ruta_objeto_s3 = '683/683_0_main_page.png'

        # Crea una instancia del cliente de S3
        s3 = boto3.client('s3', aws_access_key_id=settings.aws_access_key_id, aws_secret_access_key=settings.aws_secret_access_key, region_name=settings.region_name)

        # Descarga el objeto de S3
        nombre_archivo_local = './screenshots/descarga.png'  # Nombre del archivo local donde guardarás la imagen

        s3.download_file(settings.bucket_name, ruta_objeto_s3, nombre_archivo_local)

        print(f'Se descargó el objeto desde S3 y se guardó como "{nombre_archivo_local}".')
        
    def save_url_screenshot_s3(self, list_to_insert, db_connect_input):
        """
        This method try to connect to the DB and save the data
        :param values_dict: dictionary containing the ad_chain information
        """

        # Try to connect to the DB
        try:

            conn = psycopg2.connect(host=db_connect_input['host'],
                                    database=db_connect_input['database'],
                                    password=db_connect_input['password'],
                                    user=db_connect_input['user'],
                                    port=db_connect_input['port'])
            cursor = conn.cursor()

        except Exception as e:
            print('::DBConnect:: cant connect to DB Exception: {}'.format(e))
            raise

        else:
            sql_string = "INSERT INTO screenshots_s3( ad_event_id, screenshot_path) " \
                         "VALUES(%s,%s);"

            try:
                # Try to execute the sql_string to save the data
                cursor.executemany(sql_string, list_to_insert)
                conn.commit()
            except Exception as e:
                self.__logger.error('::Saver:: Error found trying to Save Data navigation_screenshots_s3 - {}'.format(e))

            finally:
                cursor.close()
                conn.close()

    def save_mhtml_on_s3(self, dict_mhtml, ad_event_id):
        self.__logger.info(f" --- save navigation mhtml file ---")
        
        
        # Crea instancias de cliente de S3
        s3_private = boto3.client('s3',
                                  aws_access_key_id= dict_aws_private['aws_access_key_id'],
                                  aws_secret_access_key=dict_aws_private['aws_secret_access_key'],
                                  region_name=dict_aws_private['region_name'])
        
        s3_public = boto3.client('s3',
                                 aws_access_key_id= dict_aws_public['aws_access_key_id'],
                                 aws_secret_access_key=dict_aws_public['aws_secret_access_key'],
                                 region_name=dict_aws_public['region_name'])
        
        try:
            nombre_objeto_s3 = f'{ad_event_id}/{ad_event_id}_pop_.mhtml' 
            mhtml = dict_mhtml['mhtml']                    
                       
            # Sube mhtmla a S3 privado
            s3_private.put_object(Body=mhtml, Bucket=dict_aws_private['bucket_name'], Key=nombre_objeto_s3) 
            print(f'Se subió el archivo mhtml {nombre_objeto_s3} en {dict_aws_private["bucket_name"]}.')
            
            # Sube mhtmla a S3  publico
            s3_public.put_object(Body=mhtml, Bucket=dict_aws_public['bucket_name'], Key=nombre_objeto_s3) 
            print(f'Se subió el archivo mhtml {nombre_objeto_s3} en {dict_aws_public["bucket_name"]}.')
            
            
            data_dict = {'ad_event_id':ad_event_id,
                    'mhtml_path':nombre_objeto_s3
                    }                          
        except Exception as e:
            print(f'error al subir mhtml {e}')
        # save on db_private        
        self.save_url_mhtml_s3(data_dict, db_connect)
        # save on db_public
        self.save_url_mhtml_s3(data_dict, db_connect_app)
        
    def save_url_mhtml_s3(self, data_dict, db_connect_input):
        """
        This method try to connect to the DB and save the data
        :param values_dict: dictionary containing the ad_chain information
        """

        # Try to connect to the DB
        try:

            conn = psycopg2.connect(host=db_connect_input['host'],
                                    database=db_connect_input['database'],
                                    password=db_connect_input['password'],
                                    user=db_connect_input['user'],
                                    port=db_connect_input['port'])
            cursor = conn.cursor()

        except Exception as e:
            print('::DBConnect:: cant connect to DB Exception: {}'.format(e))
            raise

        else:
            sql_string = "INSERT INTO mhtml_s3( ad_event_id, mhtml_path) " \
                         "VALUES(%s,%s);"
            data = (data_dict['ad_event_id'], data_dict['mhtml_path'])

            try:
                # Try to execute the sql_string to save the data
                cursor.execute(sql_string, data)
                conn.commit()
            except Exception as e:
                self.__logger.error('::Saver:: Error found trying to Save Data navigation_screenshots_s3 - {}'.format(e))

            finally:
                cursor.close()
                conn.close()
                
    def capture_mhtml(self, page):
        try:
            client = page.context.new_cdp_session(page)
            mhtml = client.send("Page.captureSnapshot")['data'] 
        except Exception as e:
            mhtml = None
            self.__logger.error(f'error capture mhtml {e}')
        return mhtml       
            
    def save_list_mhtml_on_s3(self, list_mhtml,ad_event_id):
        self.__logger.info(f" --- save navigation mhtml files on s3 ---")
        
        # Crea instancias de cliente de S3
        s3_private = boto3.client('s3',
                                  aws_access_key_id= dict_aws_private['aws_access_key_id'],
                                  aws_secret_access_key=dict_aws_private['aws_secret_access_key'],
                                  region_name=dict_aws_private['region_name'])
        
        s3_public = boto3.client('s3',
                                 aws_access_key_id= dict_aws_public['aws_access_key_id'],
                                 aws_secret_access_key=dict_aws_public['aws_secret_access_key'],
                                 region_name=dict_aws_public['region_name'])
        list_to_insert=[]
        for i, elem in enumerate(list_mhtml):
            try:
                
                nombre_objeto_s3 = f'{ad_event_id}/{ad_event_id}_{i}_.mhtml'
                mhtml = elem['mhtml']            

                # Sube mhtmla a S3 privado
                s3_private.put_object(Body=mhtml, Bucket=dict_aws_private['bucket_name'], Key=nombre_objeto_s3) 
                print(f'Se subió el archivo mhtml {nombre_objeto_s3} en {dict_aws_private["bucket_name"]}.')
                
                # Sube mhtmla a S3  publico
                s3_public.put_object(Body=mhtml, Bucket=dict_aws_public['bucket_name'], Key=nombre_objeto_s3) 
                print(f'Se subió el archivo mhtml {nombre_objeto_s3} en {dict_aws_public["bucket_name"]}.')
                
                
                data_dict = {'ad_event_id':ad_event_id,
                        'mhtml_path':nombre_objeto_s3
                        }  
                list_to_insert.append(data_dict)   
            except Exception as e:
                print(f'error on screenshot {e}')
        for data_dict in list_to_insert:
            # save on db_private        
            self.save_url_mhtml_s3(data_dict, db_connect)
            # save on db_public
            self.save_url_mhtml_s3(data_dict, db_connect_app)
        
