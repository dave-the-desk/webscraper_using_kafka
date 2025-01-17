import requests
from bs4 import BeautifulSoup
from confluent_kafka import Producer
import json


#Documentation: https://docs.confluent.io/platform/current/clients/confluent-kafka-python/html/index.html#pythonclient-producer


# Kafka producer configuration
conf = {
    'bootstrap.servers': 'localhost:9092',  # Replace with your Kafka broker address
    'client.id': 'web-scraping-producer'
}

# Create a Kafka producer instance
producer = Producer(conf)

# Kafka topicpi
topic = 'Movies_With_Bad_Words'


def scrapedata():
    #example url, he he he
    url = 'https://en.wikipedia.org/wiki/List_of_films_that_most_frequently_use_the_word_fuck'

    response = requests.get(url)
    soup = BeautifulSoup(response.text, 'html.parser')

    dictionary = []
    for item in soup.find_all("table"):
        tbody = item.find('tbody')
        if tbody:
           for row in tbody.find_all('tr')[1:]:  # Skip the header row
            columns = row.find_all('td')
            if len(columns) >= 3:  # Ensure there are enough columns
                title = columns[0].get_text(strip=True)
                year = columns[1].get_text(strip=True)
                rate = columns[4].get_text(strip=True)

                dictionary.append({
                    "Title": title,
                    "Year": year,
                    "Rate": rate
                })
    
    # Return the extracted data
    return dictionary
                    
                    
def send_to_kafka(movie_dictionary):
   for movie in movie_dictionary:
      #produce() creates message, we will make the key the Title
      producer.produce(topic, key='Title', value=json.dumps(movie))

      #send to kafka
      producer.flush()
        



def main():
    data = scrapedata()
    send_to_kafka(data)

if __name__ == '__main__':
    main()
