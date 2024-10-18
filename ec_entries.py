import requests
from lxml import html
import logging
import concurrent.futures
import redis
import json

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Подключение к Redis
redis_client = redis.Redis(host='localhost', port=6379, db=0)

def extract_section_content(tree, section_name):
    try:
        section_content = ""
        section_nodes = tree.xpath(f"//th[text()='{section_name}']/../following-sibling::tr")
        for node in section_nodes:
            if node.xpath(".//th"):
                break
            section_content += " ".join(node.xpath(".//text()")).strip() + "\n"
        return section_content.strip()
    except Exception as e:
        logging.error(f"Error extracting {section_name}: {e}")
        return ""

def extract_uniprot_entries(tree, section_name):
    try:
        entries = []
        section_node = tree.xpath(f"//td[text()='{section_name}']")
        if section_node:
            entry_nodes = section_node[0].xpath("./following-sibling::td[1]//a[contains(@href, 'uniprot')]")
            for entry_node in entry_nodes:
                entry_id = entry_node.get("href").split('/')[-1]
                entries.append(entry_id)
                logging.info(f"Found UniProt entry: {entry_id}")
        return "\n".join(entries)
    except Exception as e:
        logging.error(f"Error extracting {section_name}: {e}")
        return ""

def fetch_enzyme_data(ec_number, not_found_ec):
    try:
        url = f"https://enzyme.expasy.org/EC/{ec_number}"
        response = requests.get(url)
        if response.status_code == 200:
            tree = html.fromstring(response.content)
            
            record = {}
            record["EC number"] = ec_number
            
            accepted_name = extract_section_content(tree, "Accepted Name")
            alt_names = extract_section_content(tree, "Alternative Name(s)")
            entries = extract_uniprot_entries(tree, "UniProtKB/Swiss-Prot")

            if accepted_name or alt_names or entries:
                record["Accepted Name"] = accepted_name
                record["Alternative Name(s)"] = alt_names
                record["Entries"] = entries
                
                if accepted_name == "Deleted entry":
                    logging.warning(f"EC number {ec_number} is a deleted entry.")
                    not_found_ec.append(ec_number)
                    return None
                
                logging.info(f"Data found for EC number {ec_number}")
                return record

            transferred_entry_xpath = '/html/body/main/div/h3/a'
            transferred_entry_nodes = tree.xpath(transferred_entry_xpath)
            if transferred_entry_nodes:
                new_ec_number = transferred_entry_nodes[0].text.strip()
                logging.info(f"EC number {ec_number} is a transferred entry to {new_ec_number}. Redirecting...")
                return fetch_enzyme_data(new_ec_number, not_found_ec)

            logging.warning(f"No data found for EC number {ec_number}")
            not_found_ec.append(ec_number)
            return None
        
        else:
            logging.error(f"Error fetching data for EC number {ec_number}: {response.status_code}")
            return None
    
    except Exception as e:
        logging.error(f"Error fetching data for EC number {ec_number}: {e}")
        return None

def main():
    logging.info("Starting script execution")

    # Получаем EC номера из Redis
    ec_numbers_json = redis_client.get('ec_spider_results')
    if not ec_numbers_json:
        logging.error("No EC numbers found in Redis")
        return

    ec_numbers = json.loads(ec_numbers_json)
    ec_numbers = [entry['EC Number'] for entry in ec_numbers]

    not_found_ec = []
    results = []

    with concurrent.futures.ThreadPoolExecutor() as executor:
        future_to_ec = {executor.submit(fetch_enzyme_data, ec_number, not_found_ec): ec_number for ec_number in ec_numbers}
        
        for future in concurrent.futures.as_completed(future_to_ec):
            ec_number = future_to_ec[future]
            try:
                data = future.result()
                if data:
                    results.append(data)
            except Exception as e:
                logging.error(f"Error processing data for EC number '{ec_number}': {e}")
                not_found_ec.append(ec_number)

    # Сохраняем результаты в Redis
    redis_client.set('uniprot_entries', json.dumps(results))
    redis_client.set('not_found_ec', json.dumps(not_found_ec))

    logging.info("Script execution completed")

if __name__ == "__main__":
    main()