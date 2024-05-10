import constants
import re
import psycopg2
from settings import db_connect
from dependencies import log

class Ad_vast:
    def __init__(self):
        self.__logger = log.Log().get_logger(name=constants.log_file['log_name'])

    def set_and_save_vast(self, ad_event_id, list_vast):
        self.__logger.info(f" --- save vast data  ---")
        for vast in list_vast:
            try:
                vast['ad_event_id'] = ad_event_id
                ad_vast_id = self.save_ad_vast(vast)

                adsource = None
                adtaguri = None
                ad_tag_uri_domain = None
                impression = None
                impression_domain = None
                adsystem = None
                adtitle = None
                description = None
                mediafile = None
                mediafile_domain = None
                clickthrough = None
                clickthrough_domain = None
                adverification = None

                vast_content = re.findall(r'<VAST\b[^>]*>([\s\S]*?)<\/VAST>', vast['vast_content'])[0]
                # vast_content = vast['vast_content']

                # try get adsource
                try:
                    adsource = re.findall(r'(?:<vmap:AdSource)(.*?)(?:>)', vast_content)[0]
                except:
                    pass

                # try get adtaguri
                try:
                    adtaguri = re.findall(r'(?:<)(.*?)(?:</vmap:AdTagURI>)', vast_content)[0]
                except:
                    pass

                # try impression
                try:
                    impression = re.findall(r'(?:<Impression>)(.*?)(?:</Impression>)', vast_content)[0]
                    try:
                        impression_domain = re.findall(r'https?:\/\/([^\/]+)', impression)[0]
                    except:
                        pass
                except:
                    pass

                # try description
                try:
                    description = re.findall(r'(?:<Description>)(.*?)(?:</Description>)', vast_content)[0]
                except:
                    pass

                # try mediafile
                try:
                    mediafile = re.findall(r'<MediaFile[^>]*>(.*?)<\/MediaFile>', vast_content)[0]
                    try:
                        mediafile_domain = re.findall(r'https?:\/\/([^\/]+)', mediafile)[0]
                    except:
                        pass
                except:
                    pass

                # try clickthrough
                try:
                    clickthrough = re.findall(r'<MediaFile[^>]*>(.*?)<\/MediaFile>', vast_content)[0]
                    try:
                        clickthrough_domain = re.findall(r'https?:\/\/([^\/]+)', clickthrough)[0]
                    except:
                        pass
                except:
                    pass

                try:
                    adsystem = re.findall(r'(?:<AdSystem>)(.*?)(?:</AdSystem>)', vast_content)[0]
                except:
                    pass
                try:
                    adtitle = re.findall(r'(?:<AdTitle>)(.*?)(?:</AdTitle>)', vast_content)[0]
                except:
                    pass
                # try:
                #     adverification = re.findall(r'(?:<AdTitle>)(.*?)(?:</AdTitle>)', vast_content)[0]
                # except:
                #     pass
                vast_parameter_dict = {
                    "adsystem": adsystem,
                    "adtitle": adtitle,
                    'adsource': adsource,
                    'adtaguri': adtaguri,
                    'ad_tag_uri_domain': ad_tag_uri_domain,
                    'impression': impression,
                    'impression_domain': impression_domain,
                    'description': description,
                    'mediafile': mediafile,
                    'mediafile_domain': mediafile_domain,
                    'clickthrough': clickthrough,
                    'clickthrough_domain': clickthrough_domain,
                    "ad_vast_id": ad_vast_id
                }
                self.save_vast_parameter(vast_parameter_dict)
            except:
                pass

    def save_ad_vast(self, values_dict):
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

            sql_string = "INSERT INTO public.ad_vast(url, vast_content, ad_event_id) VALUES(%s, %s, %s)" \
                         " RETURNING ad_vast_id; " \


            data = (values_dict['url'], values_dict['vast_content'], values_dict['ad_event_id'])
            try:
                # Try to execute the sql_string to save the data
                cursor.execute(sql_string, data)
                ad_vast_id = cursor.fetchone()
                conn.commit()
                if ad_vast_id:
                    ad_vast_id = ad_vast_id[0]
                else:
                    ad_vast_id = None
                conn.commit()
            except Exception as e:
                self.__logger.error('::Saver:: Error found trying to Save Data ad_vast - {}'.format(e))

            finally:
                cursor.close()
                conn.close()
                return ad_vast_id

    def save_vast_parameter(self, values_dict):
        """
        This method try to connect to the DB and save the data
        :param values_dict: dictionary containing the vast parameter information
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
            sql_string = "INSERT INTO public.ad_vast_parameters (ad_system, ad_title, ad_source, ad_vast_id, " \
                         "ad_tag_uri, ad_tag_uri_domain, impression, impression_domain, description, mediafile, " \
                         "mediafile_domain, clickthrough, clickthrough_domain)" \
                         " VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) "

            data = (values_dict['adsystem'],
                    values_dict['adtitle'],
                    values_dict['adsource'],
                    values_dict['ad_vast_id'],
                    values_dict['adtaguri'],
                    values_dict['ad_tag_uri_domain'],
                    values_dict['impression'],
                    values_dict['impression_domain'],
                    values_dict['description'],
                    values_dict['mediafile'],
                    values_dict['mediafile_domain'],
                    values_dict['clickthrough'],
                    values_dict['clickthrough_domain'],
                    )
            try:
                # Try to execute the sql_string to save the data
                cursor.execute(sql_string, data)
                conn.commit()
            except Exception as e:
                self.__logger.error('::Saver:: Error found trying to Save Data vast_parameters - {}'.format(e))

            finally:
                cursor.close()
                conn.close()

