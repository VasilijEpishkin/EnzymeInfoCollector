import pandas as pd
import logging
import asyncio
from playwright.async_api import async_playwright, TimeoutError
import sys
import argparse
import redis
import json

# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Подключение к Redis
redis_client = redis.Redis(host='localhost', port=6379, db=0)

async def fetch_ec_numbers_by_name(playwright, ferment_name, existing_ec_numbers, wait_time=5000):
    try:
        # Запускаем браузер и открываем новую страницу
        browser = await playwright.chromium.launch(headless=True)
        page = await browser.new_page()

        # Устанавливаем стандартное время ожидания
        page.set_default_timeout(wait_time)

        # Переходим на страницу поиска по названию фермента
        await page.goto("https://enzyme.expasy.org/enzyme-byname.html")

        # Ждём загрузки поля ввода
        await page.wait_for_selector('xpath=/html/body/main/div/center/form/input[1]')

        # Вводим название фермента и нажимаем на кнопку поиска
        await page.fill('xpath=/html/body/main/div/center/form/input[1]', ferment_name)
        await page.click('xpath=/html/body/main/div/center/form/input[2]')

        # Попытка ожидания загрузки таблицы результатов или проверки на наличие сообщения об ошибке
        try:
            await page.wait_for_selector("//table[@class='type-1']//tr", timeout=wait_time)

            # Проверка на наличие сообщения об отсутствии результатов
            if await page.is_visible("text=No ENZYME entry was found with name containing"):
                logging.warning(f"Результаты для '{ferment_name}' не найдены.")
                await page.close()
                await browser.close()
                return []

            # Если таблица найдена, извлекаем данные
            rows = await page.query_selector_all("//table[@class='type-1']//tr")
            ec_results = {}

            for row in rows:
                ec_number_element = await row.query_selector("td:nth-child(1) > a")
                ec_number = (await ec_number_element.text_content()).strip() if ec_number_element else None

                # Проверяем, не является ли EC номер дубликатом
                if ec_number in existing_ec_numbers:
                    logging.info(f"Дубликат EC номера '{ec_number}' обнаружен, он будет пропущен.")
                    continue

                descriptions_element = await row.query_selector("td:nth-child(2)")
                descriptions = (await descriptions_element.text_content()).strip().split('\n') if descriptions_element else []

                # Удаляем первые три символа у каждого названия фермента
                descriptions = [desc[3:].strip() for desc in descriptions if len(desc) > 3]

                # Объединяем названия ферментов для одного EC номера в одну строку
                ec_results[ec_number] = descriptions
                existing_ec_numbers.add(ec_number)

            formatted_results = [{"EC Number": ec_number, "Protein": "\n".join(descriptions)}
                                 for ec_number, descriptions in ec_results.items()]

            await page.close()
            await browser.close()
            return formatted_results

        except TimeoutError:
            logging.warning(f"Результаты для '{ferment_name}' не найдены.")
            await page.close()
            await browser.close()
            return []

    except Exception as e:
        logging.error(f"Ошибка при поиске EC номеров для '{ferment_name}': {e}")
        return []

async def process_input(ferment_names):
    results = []
    not_found = []
    existing_ec_numbers = set()

    async with async_playwright() as playwright:
        for index, name in enumerate(ferment_names):
            ec_numbers = await fetch_ec_numbers_by_name(playwright, name, existing_ec_numbers)
            if ec_numbers:
                results.extend(ec_numbers)
            else:
                not_found.append({"Protein": name})

    return results, not_found

def save_results_to_redis(results, not_found):
    if results:
        redis_client.set('names_ec_results', json.dumps(results))
        logging.info("Результаты сохранены в Redis под ключом 'names_ec_results'.")
    if not_found:
        redis_client.set('not_found_results', json.dumps(not_found))
        logging.info("Ферменты без EC номеров сохранены в Redis под ключом 'not_found_results'.")

def parse_arguments():
    parser = argparse.ArgumentParser(description="Парсер ферментов с сайта ExPASy")
    parser.add_argument('ferment', type=str, help="Название фермента для поиска")
    return parser.parse_args()

async def main():
    args = parse_arguments()
    ferment_names = [args.ferment]
    results, not_found = await process_input(ferment_names)
    save_results_to_redis(results, not_found)

if __name__ == "__main__":
    asyncio.run(main())