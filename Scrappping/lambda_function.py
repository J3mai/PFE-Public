## IMPORTATIONS ##
##################

import os
import time
from selenium import webdriver
import pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import re
from lxml import etree
import boto3
import io

## Configuration ##
###################

#Selenium driver settings
headers = {
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
    'Accept-Encoding': 'gzip, deflate, br, zstd',
    'Accept-Language': 'fr-FR,fr;q=0.9,en-US;q=0.8,en;q=0.7,ar;q=0.6',
    'Cache-Control': 'max-age=0',
    'Cookie': 'G_AUTHUSER_H=0; newPath=New-York; split=n; split_tcv=123; __vst=60a1abe9-92c5-4fd4-9600-6c266e5b159d; __ssn=cdbd728a-108d-4d04-9fa9-193201afd2a6; __ssnstarttime=1707907525; __bot=false; permutive-id=ef6e713b-9a00-4dba-8874-c34e07a36bd1; AMCVS_8853394255142B6A0A4C98A4%40AdobeOrg=1; pxcts=2aeb0267-cb26-11ee-b449-adaf9786cd16; _pxvid=2aeaf5a6-cb26-11ee-b449-eb16cc5afca0; s_ecid=MCMID%7C53602318338276537142020873590065126194; _gcl_au=1.1.1551499233.1707907529; _scid=f8ff3d54-1f0e-45b7-b47e-495e6e5983d7; __split=26; G_ENABLED_IDPS=google; _ncg_id_=446692e0-3e4b-4430-950e-56cae0a089d9; _tt_enable_cookie=1; _ttp=iTjDMrIjRsXCX3WuMMc3UXPtiye; AMCVS_AMCV_8853394255142B6A0A4C98A4%40AdobeOrg=1; _ncg_domain_id_=446692e0-3e4b-4430-950e-56cae0a089d9.1.1707907532093.1770979532093; __qca=P0-94666805-1707907532051; _ncg_g_id_=61dc3234-e0b0-4f2f-995f-55eb24580909.3.1707907534.1770979532093; ajs_anonymous_id=a769ae9a-2b32-47b8-a46d-2e575ed02dc6; mdLogger=false; kampyle_userid=7121-4249-4698-43e9-ce7e-9eb0-7880-1be5; _lr_env_src_ats=false; g_state={"i_l":0}; lithium_display_name=false; G_AUTHUSER_H=0; __gsas=ID=20b53467d5886cbb:T=1707907961:RT=1707907961:S=ALNI_MZ-2j9iaqzh_tHc1MibV9CWaGB36g; DECLINED_DATE=1707907997675; isRVLExists=true; _pbjs_userid_consent_data=3524755945110770; _cc_id=2c07958ca1ccf5576ade8499106dc681; __gads=ID=a7d58e01bdddfe22:T=1707921134:RT=1707971176:S=ALNI_MZcl6lpRKPeImT37DRqtBLGPYUWEQ; __gpi=UID=00000d205a0e80a7:T=1707921134:RT=1707971176:S=ALNI_MZ_ayJpS7r9hqOmQm9QHU-w-ryAJg; __eoi=ID=4d0dcb16e7124c29:T=1707921134:RT=1707971176:S=AA-AfjYSSd3sqbKDgwrWYRMNrrAT; kampylePageLoadedTimestamp=1707971226393; _tac=false~self|not-available; _ta=us~1~c633ec0672eeb80ac2319af62d9003ff; _sctr=1%7C1709247600000; _gid=GA1.2.1852671914.1709637407; cto_bundle=bZ1DmF9yZ3Baa1luSU8xcWJ1UEZSNnhPdjdLSWQzNFhRUDBVOERXSXphSGlJZ2FZMGgySldXQ1lmanNtdWZMMTVNbnlWaFElMkZwWnY5bmRjM1ZsZ21vQVRiVXl0bGxpeFgwUCUyRnFuSXpKd0UlMkYlMkY5aTFRZVpSaEFCWFo1bFh3bjY3V2o1YThBdjRVVmxWZ0NaM01xOW9XNzg3cUFNZyUzRCUzRA; _lr_sampling_rate=0; srchID=c37157226b884c2999e4177892c0757e; _iidt=mAsWuiv6zdMlrpvVDI90mBCqZZ449/okr4KBh7O920qVocWhHnVeNL0guiCD83SYF7MR8PmrIWvDUshWPsPRP5+JfNBAT1RqK943dw==; _vid_t=Yhcqk61iDdieagF9l7RBlbg+FffjD+x0R42TfybwNJXWUZAsi/6QrcotstLxOBA2dJa9ry53cpgzwFFajrfvK9yVE3wWiiV4XXXRpw==; __fp=5WkRs74UuQnxatRfttdB; criteria=sprefix%3D%252Fnewhomecommunities%26area_type%3Dstate%26pg%3D1%26state_code%3DAL%26state%3DAlabama%26state_id%3DAL%26loc%3DAlabama%26locSlug%3DAlabama; AMCV_8853394255142B6A0A4C98A4%40AdobeOrg=-1124106680%7CMCIDTS%7C19788%7CMCMID%7C53602318338276537142020873590065126194%7CMCAAMLH-1710321707%7C6%7CMCAAMB-1710321707%7CRKhpRz8krg2tLO6pguXWp5olkAcUniQYPHaMWWgdJ3xzPWQmdj0y%7CMCOPTOUT-1709724107s%7CNONE%7CMCAID%7CNONE%7CvVersion%7C5.2.0%7CMCCIDH%7C-1284638780; _ncg_sp_ses.cc72=*; ab.storage.deviceId.7cc9d032-9d6d-44cf-a8f5-d276489af322=%7B%22g%22%3A%22fdde6a09-d196-bf61-21ef-6c1bc979fc72%22%2C%22c%22%3A1707907535978%2C%22l%22%3A1709716909330%7D; ab.storage.userId.7cc9d032-9d6d-44cf-a8f5-d276489af322=%7B%22g%22%3A%22visitor_60a1abe9-92c5-4fd4-9600-6c266e5b159d%22%2C%22c%22%3A1709646281768%2C%22l%22%3A1709716909330%7D; ab.storage.sessionId.7cc9d032-9d6d-44cf-a8f5-d276489af322=%7B%22g%22%3A%22a2b15fe4-9a6d-2fe5-e7f3-bdbeab3e39ad%22%2C%22e%22%3A1709718709335%2C%22c%22%3A1709716909329%2C%22l%22%3A1709716909335%7D; _rdt_uuid=1707907530173.231b5c20-9240-4130-a8a4-f5bf00dfd947; _scid_r=f8ff3d54-1f0e-45b7-b47e-495e6e5983d7; _ncg_sp_id.cc72=446692e0-3e4b-4430-950e-56cae0a089d9.1707907532.11.1709716910.1709646281.df0ce822-476f-49c2-837c-fcc4e8e6bac5; _tas=tuhb02obtgr; AMCV_AMCV_8853394255142B6A0A4C98A4%40AdobeOrg=-1124106680%7CMCMID%7C53602318338276537142020873590065126194%7CMCIDTS%7C19788%7CMCOPTOUT-1709724109s%7CNONE%7CvVersion%7C5.2.0; adcloud={%22_les_v%22:%22y%2Crealtor.com%2C1709718710%22}; _ga=GA1.2.1761330529.1707907532; _gat=1; _px3=113b123ff5528b0dfdaa9858ff1ef6b7805594fa8da5a5f445ab24823f5a9dc3:BKDAGCTV1tsgHz2M8SrwGvuuHelw1Otq7aO6HymewHblMCDZ0nzd6f2VJd6M4uFDwcbdT/5kB0V6XU0KRxifHg==:1000:NTWvqS2UbC9FEs8tosu7f6q886EFWSY5vFL4gRG6J+cz3fl4eL9n4IrH41G0ywwxZ3Ry4Z9ZMAtK4HPBmm1RuHvhP3zbWEB5TDhEImHXyduOJgcmx7vwk/4N3h7LpklwAmm5zoVCrfBf5GQIU1TnuPlS/zYmC3jdQIbTqPGJ/KXGO/RsGzLKVpJRR4ETYEwuBFG6e7r7zaZApX4JTFJF7gTfOVNbKU7eS7VvjK7ESH4=; kampyleUserSession=1709716912517; kampyleUserSessionsCount=3670; kampyleSessionPageCounter=1; REMEMBER_ME=em=LnYGTFkeKLwE4Y1CbmJ708Z7JxvgxrH2nWLD9LX7+7M=&regID=8206cb35-9c9c-4637-af38-cf3056b7ef09&nm=jemaighassen1&pud=&pID=65cc9b10ed65040187c3d1c1&stat=1&act=True&yob=&opt=20&exp=9/11/2024 09:22:02 AM&up=3/6/2024 09:22:02 AM&; _ga_MS5EHT6J6V=GS1.1.1709716910.10.0.1709716919.0.0.0; _uetsid=db08e750dae111ee9d0f0965289516cc|sns4el|2|fju|0|1525; _uetvid=2f847d40cb2611eeaac24522263ad3ba|1564qbq|1709716914247|1|1|bat.bing.com/p/insights/c/o',
    'Sec-Ch-Ua': '"Chromium";v="122", "Not(A:Brand";v="24", "Google Chrome";v="122"',
    'Sec-Ch-Ua-Mobile': '?0',
    'Sec-Ch-Ua-Platform': "Windows",
    'Sec-Fetch-Dest': 'document',
    'Sec-Fetch-Mode': 'navigate',
    'Sec-Fetch-Site': 'none',
    'Sec-Fetch-User': '?1',
    'Upgrade-Insecure-Requests': '1',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
}

#Kafka configuration
KAFKA_SERVER = "localhost:9092"
TOPIC = "production"

#Hadoop configuration
HADOOP_SERVER = "http://localhost:50070" # or 9870

#PostgreSql Configuration
HOST = "localhost:5432"
DATABASE = "DW"
TABLE = "public.offers"
USER = "postgres"
PASSWORD = "root_password"
DRIVER = "org.postgresql.Driver"
WRITING_MODE = "append" #overwrite

## FUNCTIONS ##
###############


cities = [
    "/new-york-ny","/miami-fl", "/san-jose-ca", "/arlington-va", "/san-diego-ca", "/washington-dc","/los-angeles-ca", "/chicago-il", "/santa-ana-ca", "/anaheim-ca", "/oakland-ca", "/charleston-sc",
        "/fort-lauderdale-fl", "/urban-honolulu-hi", "/seattle-wa", "/denver-co", "/long-beach-ca",
        "/new-haven-ct", "/atlanta-ga", "/scottsdale-az", "/nashville-tn", "/providence-ri",
        "/tampa-fl", "/gilbert-az", "/orlando-fl", "/new-orleans-la",
        "/new-jersey-nj", "/durham-nc", "/madison-wi", "/portland-or", "/minneapolis-mn", "/phoenix-az", "/irving-tx",
        "/anchorage-ak", "/cleveland-oh",
        "/raleigh-nc", "/norfolk-va", "/fort-worth-tx", "/reno-nv", "/houston-tx", "/albuquerque-nm", "/el-paso-tx",
        "/augusta-ga", "/winston-salem-nc", "/las-vegas-nv", "/mesa-az", "/glendale-az", "/jacksonville-fl",
        "/pittsburgh-pa",
        "/chattanooga-tn", "/buffalo-ny", "/columbus-oh", "/colorado-springs-co", "/st-petersburg-fl", "/sacramento-ca",
        "/austin-tx",
        "/oklahoma-city-ok", "/lincoln-ne", "/akron-oh", "/shreveport-la", "/bakersfield-ca", "/kansas-city-mo",
        "/arlington-tx", "/syracuse-ny", "/milwaukee-wi", "/spokane-wa",
        "/detroit-mi", "/san-antonio-tx", "/louisville-ky", "/tallahassee-fl", "/indianapolis-in", "/baton-rouge-la",
        "/cincinnati-oh", "/lexington-ky", "/omaha-ne",
        "/greensboro-nc", "/des-moines-ia", "/tucson-az", "/tulsa-ok", "/wichita-ks", "/san-francisco-ca", "/boston-ma",
        "/st-louis-mo","/newark-nj","/henderson-nv", "/virginia-beach-va", "/charlotte-nc", "/philadelphia-pa", "/memphis-tn", "/rochester-ny",
        "/plano-tx", "/aurora-co", "/salt-lake-city-ut", "/boise-id", "/baltimore-md", "/dallas-tx", "/fresno-ca",
        "/richmond-va", "/knoxville-tn", "/asheville-nc"
        

]

def parse_relative_time(relative_time):
    # Regular expression to match "X+ days ago" or "X days ago"
    match = re.match(r"(\d+)\+?\s*days? ago", relative_time.lower())
    if match:
        # Extract the number of days
        quantity = int(match.group(1))
        # Calculate the timedelta
        delta = timedelta(days=quantity)
    else:
        # Split the input string to extract the quantity and unit
        quantity, unit, _ = relative_time.split()
        # Convert quantity to an integer
        quantity = int(quantity)
        # Convert unit to lowercase for case-insensitive comparison
        unit = unit.lower()
        # Determine the timedelta based on the unit
        if unit == "second" or unit == "seconds":
            delta = timedelta(seconds=quantity)
        elif unit == "minute" or unit == "minutes":
            delta = timedelta(minutes=quantity)
        elif unit == "hour" or unit == "hours":
            delta = timedelta(hours=quantity)
        elif unit == "day" or unit == "days":
            delta = timedelta(days=quantity)
        elif unit == "week" or unit == "weeks":
            delta = timedelta(weeks=quantity)
        elif unit == "month" or unit == "months":
            # Approximate months to 30 days
            delta = timedelta(days=quantity * 30)
        elif unit == "year" or unit == "years":
            # Approximate years to 365 days
            delta = timedelta(days=quantity * 365)
        else:
            raise ValueError("Invalid unit provided")
    # Calculate the datetime by subtracting the timedelta from the current time
    result = datetime.now() - delta
    # Truncate microseconds
    result = result.replace(microsecond=0)
    return result



def set_driver():
    options = webdriver.ChromeOptions()
    for key, value in headers.items():
        options.add_argument(f"--{key}={value}")
    options.add_argument("--headless")
    options.add_argument('log-level=3')
    options.add_argument("disable-infobars")
    options.add_argument("--disable-extensions")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-application-cache")
    # options.add_argument("--disable-gpu")
    options.add_argument("--disable-dev-shm-usage")
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_argument("--ignore-ssl-errors=yes")
    options.add_experimental_option("useAutomationExtension", False)
    driver = webdriver.Chrome(options=options)
    return driver


def crawl(links):
    driver = set_driver()
    for state in cities:
        print(state)
        url = f"https://www.zumper.com/apartments-for-rent{state}?sort=newest"
        try:
            driver.get(url)
            x = 1
        except Exception:
            print(Exception)
            continue
        finally:
            button_exists = True
            while button_exists and x < 3:
                print(f"page {x} at {datetime.now()}")
                x += 1
                time.sleep(2)
                for _ in range(20):
                    driver.execute_script("scrollBy(0,500)")
                    time.sleep(0.15)
                soup = BeautifulSoup(driver.page_source, "html.parser")
                div = soup.find("div", attrs={"class": "css-9ulpxd"})
                if div:
                    offers = div.find_all("a", attrs={"class": "DetailPageLink"})
                    if len(offers) > 0 :#and 10 < x <= 15
                        for offer in offers:
                            links.append(offer.get("href"))
                pagination = soup.find("div", attrs={"class": "css-jsvuql"})
                if pagination:
                    next_btn = pagination.find(
                        "a", attrs={"class": "chakra-button css-1ta7ioi"}
                    )
                    if next_btn :
                        next_btn_href = next_btn.get("href")
                        driver.get("https://www.zumper.com" + next_btn_href)
                    else:
                        button_exists = False
                else:
                    button_exists = False
    driver.quit()


def get_data(links):
    driver = set_driver()
    Monthly_rent = []
    Beds = []
    Baths = []
    Surface = []
    addresses = []
    Posted = []
    progress_width = 100
    stop_index = 0
    try : 
        for i, link in enumerate(links):
            stop_index = i
            progress = (i + 1) / len(links)
            num_hashes = int(progress * progress_width)-1
            progress_bar = "#" * num_hashes + "-" * (progress_width - num_hashes)
            print(f"\r{i}/{len(links)}:[{progress_bar}] {progress:.1%}", end="", flush=True)
            url = "https://www.zumper.com" + link
            try:
                driver.get(url)
                soup = BeautifulSoup(driver.page_source, "lxml")
                root = etree.HTML(str(soup))
                address1 = root.xpath(
                    '//*[@id="root"]/div/div/div[1]/div[2]/div/section/div[1]/div[2]/div/div[1]/div/div[3]/address')
                address2 = root.xpath(
                    '//*[@id="root"]/div/div/div[1]/div[2]/section/div[1]/div/div[1]/div[2]/div/div[1]/h1/span/address')
                address3 = root.xpath(
                    '//*[@id="root"]/div/div/div[1]/div[2]/div/section/div[1]/div[2]/div/div[1]/div/div[2]/address')
                if address1:
                    address = address1[0].text
                elif address2:
                    address = address2[0].text
                elif address3:
                    address = address3[0].text
                else:
                    address = None

                posted_on = soup.find("div", attrs={"class": "css-7qlyqh"})
                table = soup.table.find_all("tr")
                Monthly_rent.append(table[0].find("td").text.replace(",", ""))
                Beds.append(table[1].find("td").text.replace(",", ""))
                Baths.append(table[2].find("td").text.replace(",", ""))
                Surface.append(table[3].find("td").text.replace(",", ""))
                addresses.append(address)
                Posted.append(str(parse_relative_time(posted_on.text)))
            except Exception as e:
                print(f" An error occurred: {e} at link {link}")
    except Exception as e:
        print(e)
    finally:
        # data = []
        # for i in range (len(Monthly_rent)):
        #     data.append({
        #     "Monthly rent": Monthly_rent[i],
        #     "Beds": Beds[i],
        #     "Baths": Baths[i],
        #     "Surface": Surface[i],
        #     "address": addresses[i],
        #     "Posted": Posted[i],
        # })

        data = pd.DataFrame(
            {
                "Monthly rent": Monthly_rent,
                "Beds": Beds,
                "Baths": Baths,
                "Surface": Surface,
                "address": addresses,
                "Posted": Posted,
            }
        )
        return (stop_index, data)

def lambda_handler(event, context):

    s3 = boto3.client('s3',
                  aws_access_key_id=os.environ["aws_access_key_id"],
                  aws_secret_access_key=os.environ["aws_secret_access_key"],
                  region_name='eu-west-3')
    # s3 = boto3.client('s3')

    links = []
    crawl(links)
    print(f"##########   GETTING DATA FROM {len(links)} SOURCES    ########## ")
    index,data = get_data(links)
    current_date = datetime.now().strftime('%Y-%m-%d')
    bucket_name = 'talan-pfe-etl-data-lake'
    file_key = f'data/raw-data/daily/{current_date}/{current_date}.parquet'#.json'
    
    # Convert the dictionary to a JSON string
    # json_string = json.dumps(data)
    parquet_buffer = io.BytesIO()
    df = data.to_parquet(parquet_buffer, index=False)
    try:
        s3.put_object(Body=parquet_buffer.getvalue(), Bucket=bucket_name, Key=file_key) #parquet_buffer.getvalue()
        print(f"\nParquet file uploaded successfully to s3://{bucket_name}/{file_key}")
    except Exception as e:
        print(f"\nError uploading Parquet file to S3: {e}")


e={}
c={}

lambda_handler(e,c)

