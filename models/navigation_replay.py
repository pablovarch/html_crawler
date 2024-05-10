from dependencies import log, tools
from models import sequence_url, navigation_screenshot
import constants
import psycopg2
from settings import db_connect, screenshot

class Navigation_replay:

    def __init__(self):
        self.__logger = log.Log().get_logger(name=constants.log_file['log_name'])
        self.__navigation_screenshot = navigation_screenshot.Navigation_screenshot()
        self.__sequence_url = sequence_url.Sequence()       

    def get_domain_to_replay(self):
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
            sql_string = """-- Weekly Ad Sniffer Scans
                            -- Updated 2023.09.27

                            -- Summary
                            -- Creates a list of sites and target countries prioritized for by ad traffic
                            -- Domain criteria is new cases & cases 7+ days old. Source is most recent month of SimilarWeb data.
                            -- Selecting cases 7+ days old gives teams time to process, and ensures we don't scan domains before needed

                            WITH
                            query_variables as (
                                SELECT
                                    (SELECT max(site_intel_query.query_start) FROM site_intel_query) as latest_sw_month,
                                    1 as tenant_id
                            )
                            ,
                            sw_domains AS (
                            -- Build list of sites queried since past month
                                SELECT DISTINCT
                                    dim_traffic.domain_id,
                                    avg(dim_traffic.traffic)::BIGINT as global_traffic
                                FROM
                                    dim_traffic
                                    , query_variables
                                WHERE
                                    dim_traffic."month"::DATE >= query_variables.latest_sw_month - INTERVAL '2' MONTH
                                    AND dim_traffic.domain_country = 'World'
                                GROUP BY
                                    dim_traffic.domain_id
                                ORDER BY
                                    dim_traffic.domain_id ASC
                            )
                            ,
                            active_case_domains AS (
                            -- 	Build list of sites with active cases < 7 days old
                            -- 	Active cases do not need to be re-scanned for 7 days.
                            -- 	Note on Cases for 2 companies:
                                -- If an active case for 2 companies is reported to company A, not B, we can still pause scanning for the domain.
                                -- The case reporting query picks up all domains active 7+ days since the last report, so company B will get a report for that domain even if we pause scanning.
                                SELECT
                                    DISTINCT case_status_domain.domain_id
                                FROM
                                    case_status_domain
                                    JOIN case_reports on case_status_domain.case_report_id=case_reports.case_report_id
                                    , query_variables
                                WHERE
                                    -- List all domains reported since last Monday, inclusive
                                    case_reports.report_date::DATE >= date_trunc('week', CURRENT_DATE - INTERVAL '7' DAY)::DATE
                                GROUP BY
                                    case_status_domain.domain_id
                                ORDER BY
                                    case_status_domain.domain_id ASC
                            )
                            ,
                            client_domains as (
                                SELECT
                                    tenants_domain.domain_id
                                FROM
                                    tenants_domain
                                    , query_variables
                                WHERE
                                    tenants_domain.tenant_id = query_variables.tenant_id
                            )
                            ,
                            top_countries as (
                                SELECT DISTINCT
                                    dim_traffic.domain_country,
                                    sum(dim_traffic.traffic) as total_country_traffic,
                                    RANK() OVER(ORDER by sum(dim_traffic.traffic) DESC) as country_global_rank
                                FROM
                                    dim_traffic
                                    JOIN country_data on dim_traffic.domain_country = country_data.country
                                    , query_variables
                                WHERE
                                    dim_traffic."month"::DATE = query_variables.latest_sw_month
                                    AND dim_traffic.domain_country != 'World'
                                    AND country_data."rank" is not NULL
                                GROUP BY
                                    dim_traffic.domain_country
                                ORDER BY
                                    sum(dim_traffic.traffic) DESC
                            )
                            ,
                            scan_domains as (
                            -- List of sites in sw_domains and NOT in active_case_domains + basic domain criteria
                                SELECT
                                    sw_domains.domain_id,
                                    sw_domains.global_traffic,
                                    RANK() OVER(ORDER by sw_domains.global_traffic DESC) as global_rank
                                    FROM
                                    sw_domains
                                    JOIN client_domains on sw_domains.domain_id = client_domains.domain_id
                                    LEFT JOIN active_case_domains ON sw_domains.domain_id = active_case_domains.domain_id
                                    JOIN domain_attributes on sw_domains.domain_id = domain_attributes.domain_id
                                WHERE
                                    -- No active cases from previous week
                                    active_case_domains.domain_id is NULL
                                    -- Online domains
                                    AND domain_attributes.online_status = 'Online'
                                    -- Domain Classifications:
                                    -- Sys Classified as Enforce or Review
                                    -- or Analyst Classified as Enforce
                                    AND ((domain_attributes.analyst_classification_id = 1)
                                    OR ((domain_attributes.domain_classification_id = 1
                                        OR domain_attributes.domain_classification_id = 2
                                        OR domain_attributes.domain_classification_id = 3)
                                        AND domain_attributes.analyst_classification_id is NULL))
                                ORDER BY
                                    sw_domains.domain_id ASC
                            )
                            ,
                            scan_list AS (
                            -- List of scans, ie domain + country
                                SELECT
                                    -- Generate country rank by ranking countries per domain in order of traffic
                                    domain_attributes.domain_id, 
                                    domain_attributes.domain AS site,
                                    dim_traffic.domain_country,
                                    domain_attributes.online_status,
                                    avg(dim_traffic.traffic)::BIGINT AS country_traffic,
                                    DENSE_RANK() OVER(PARTITION BY domain_attributes. "domain" ORDER BY avg(dim_traffic.traffic)::BIGINT DESC) as country_rank,
                                    scan_domains.global_traffic,
                                    scan_domains.global_rank
                                FROM
                                    scan_domains
                                    JOIN domain_attributes ON scan_domains.domain_id = domain_attributes.domain_id
                                    LEFT JOIN dim_traffic ON scan_domains.domain_id = dim_traffic.domain_id
                                    JOIN top_countries on dim_traffic.domain_country = top_countries.domain_country
                                WHERE
                                    -- Exclude World as a country for targeting
                                    dim_traffic.domain_country != 'World'
                            -- 		-- Past 90 days of SimilarWeb Data
                                    AND dim_traffic."month" >= date_trunc('month', CURRENT_DATE - INTERVAL '90' DAY)
                                GROUP by
                                    domain_attributes.domain_id,
                                    scan_domains.global_rank,
                                    domain_attributes. "domain",
                                    dim_traffic.domain_country,
                                    domain_attributes.online_status,
                                    scan_domains.global_traffic,
                                    scan_domains.global_rank
                                ORDER BY
                                    domain_attributes. "domain" ASC
                            )
                            SELECT ae.domain_id,
                            da.domain ,ae.event_date,  bus.sequence_num , bus.url 
                            FROM browser_profiles bp 
                            JOIN ad_events ae 
                            ON bp.browser_profile_id = ae.browser_profile_id 
                            JOIN browser_urls_seq bus 
                            ON bus.ad_event_id = ae.ad_event_id 
                            join domain_attributes da 
                            on da.domain_id = ae.domain_id 
                            WHERE bp.crawler_method ='ForensicPlaywright 2023.08_auto' 
                            AND ae.is_popup = false
                            AND ae.event_date = (
                                SELECT MAX(event_date) 
                                FROM ad_events 
                                WHERE browser_profile_id = bp.browser_profile_id)
                                and ae.domain_id in (select distinct domain_id from scan_list)
                                and bus.sequence_num =1
                            ; """

            try:
                # Try to execute the sql_string to save the data
                cursor.execute(sql_string)
                respuesta = cursor.fetchall()
                conn.commit()
                if respuesta:
                    list_domain_replay = []
                    for elem in respuesta:
                        domain_data = {
                            'domain_id': elem[0],
                            'domain': elem[1],
                        }
                        list_domain_replay.append(domain_data)
                else:
                    list_domain_replay = []

            except Exception as e:
                self.__logger.error('::Saver:: Error found trying to get_all_domain_attributes - {}'.format(e))

            finally:
                cursor.close()
                conn.close()
                return list_domain_replay
            
    def navigation_replay(self,page, domain_id):
        list_screenshots_traffic = []
        
        self.__logger.info(f'--try to replay sequence --')
        # get list sequence url
        list_sequence_url = self.__sequence_url.get_sequence_from_domain_id(domain_id)
        # navigation for each url
        if list_sequence_url:
            for num, sequence_url in enumerate(list_sequence_url):
                try:
                    page.goto(sequence_url)
                    page.wait_for_load_state(timeout=20000)
                    if screenshot:
                        screenshot_ = self.__navigation_screenshot.take_screenshot(page)
                        dict_screenshot = {
                            'name': f'sequence_url_{num}',
                            'screenshot': screenshot_}
                        list_screenshots_traffic.append(dict_screenshot)
                except Exception as e:
                    print(f'error in replay sequence {e}')
                     
        return list_screenshots_traffic 
    
    def check_domain_in_navigation_replay(self, list_all_navigation_replay, domain_id, crawler_mode):
         for elem in list_all_navigation_replay:
             if elem['domain_id'] == domain_id:
                crawler_mode = 'NavigationReplayPlaywright' 
                return crawler_mode        
         return crawler_mode
         
