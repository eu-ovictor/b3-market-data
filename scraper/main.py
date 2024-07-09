import aiofiles
import aiohttp
import asyncio
import os

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from tqdm import tqdm

BASE_URL = "https://www.b3.com.br/pt_br/market-data-e-indices/servicos-de-dados/market-data/cotacoes/cotacoes/"
ASSETS_PATH = "./assets/"


async def fetch_market_data_file(session, url, pbar):
    async with session.get(url) as response:
        if response.status == 200:
            file_name = url.split("/")[-1] + ".zip"

            file_path = os.path.join(ASSETS_PATH, file_name)

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

    driver.get(BASE_URL)

    iframe = driver.find_element(By.TAG_NAME, "iframe")

    driver.switch_to.frame(iframe)

    links = [
        link.get_attribute("href")
        for link in driver.find_elements(By.TAG_NAME, "a")
        if "tickercsv" in link.get_attribute("href")
    ]

    with tqdm(
        total=len(links),
        desc="Obtendo arquivos de negócios",
        unit=" arquivos",
    ) as pbar:
        async with aiohttp.ClientSession() as session:
            await asyncio.gather(*[fetch_market_data_file(session, link, pbar) for link in links])

if __name__ == "__main__":
    asyncio.run(main())
