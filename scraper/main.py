import aiofiles
import aiohttp
import asyncio
import os

from datetime import date, timedelta
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from tqdm import tqdm

B3_URL = os.getenv("B3_URL", "https://www.b3.com.br/pt_br/market-data-e-indices/servicos-de-dados/market-data/cotacoes/cotacoes/")
DOWNLOADS_DIR = os.getenv("DOWLOADS_DIR", "downloads")
OFFSET = int(os.getenv("OFFSET", 7))


async def fetch_market_data_file(session, url, pbar):
    async with session.get(url) as response:
        if response.status == 200:
            file_name = url.split("/")[-1] + ".zip"

            file_path = os.path.join(os.getcwd(), DOWNLOADS_DIR, file_name)

            async with aiofiles.open(file_path, "wb") as f:
                await f.write(await response.content.read())

            os.chmod(file_path, 0o777)

            pbar.update(1)


async def main():
    chrome_options = Options()

    chrome_options.add_argument('--headless')
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--no-sandbox")

    driver = webdriver.Chrome(service=Service(), options=chrome_options)

    driver.get(B3_URL)

    iframe = driver.find_element(By.TAG_NAME, "iframe")

    driver.switch_to.frame(iframe)

    dates = []

    for i in range(1, 8):
        dt = date.today() - timedelta(days=i)
        dates.append(dt.strftime('%Y-%m-%d'))

    links = []

    for link in driver.find_elements(By.TAG_NAME, "a"):
        href = link.get_attribute("href")

        if href and "tickercsv" in href:
            reference_date = href.split("/")[-1]

            if reference_date in dates:
                links.append(href)

    with tqdm(
        total=len(links),
        desc="Obtendo arquivos de negócios",
        unit=" arquivos",
    ) as pbar:
        async with aiohttp.ClientSession() as session:
            await asyncio.gather(*[fetch_market_data_file(session, link, pbar) for link in links])

if __name__ == "__main__":
    asyncio.run(main())
