import json
import uuid

import scrapy
from geopy.distance import geodesic

from locations.items import GeojsonPointItem


class McdonaldsIESpider(scrapy.Spider):
    name = "mcdonalds_ie"
    brand_name = "McDonalds"
    spider_chain_id = "1566"
    country_code = "IE"
    max_items = 92  # Limit the number of items to 92
    items_scraped = 0  # Counter to track the number of items scraped

    custom_settings = {
        'ITEM_PIPELINES': {
            'locations.pipelines.DuplicatesPipeline': 200,
            'locations.pipelines.ApplySpiderNamePipeline': 250,
            'locations.pipelines.ApplySpiderLevelAttributesPipeline': 300,
        },
        'FEED_FORMAT': 'geojson',
        'FEED_URI': 'mcdonalds_locations_ie.geojson'
    }

    def start_requests(self):
        # Bounding box for Ireland
        ireland_bbox = {
            'north': 55.381926,  # Northernmost point
            'south': 51.424489,  # Southernmost point
            'east': -5.442573,   # Easternmost point
            'west': -10.664824   # Westernmost point
        }

        # Generate points within the bounding box
        points = self.generate_points(ireland_bbox, 30)

        for point in points:
            url = f"https://www.mcdonalds.com/googleappsv2/geolocation?latitude={point[0]}&longitude={point[1]}&radius=50&maxResults=50&country=ie&language=en-ie"
            self.logger.info(f"Requesting URL: {url}")
            yield scrapy.Request(url, callback=self.parse)

    def generate_points(self, bbox, num_points):
        """
        Generate a list of latitude and longitude points within the bounding box.
        """
        lat_range = bbox['north'] - bbox['south']
        lon_range = bbox['east'] - bbox['west']

        lat_step = lat_range / (num_points ** 0.5)
        lon_step = lon_range / (num_points ** 0.5)

        points = []
        for i in range(int(num_points ** 0.5)):
            for j in range(int(num_points ** 0.5)):
                lat = bbox['south'] + i * lat_step
                lon = bbox['west'] + j * lon_step
                points.append((lat, lon))
        return points

    def parse(self, response):
        self.logger.info("Parsing response")
        self.logger.debug(f"Raw response: {response.text}")

        try:
            data = json.loads(response.text)
            self.logger.debug(f"Parsed JSON data: {data}")
        except json.JSONDecodeError:
            self.logger.error("Failed to parse JSON response")
            return

        if not data.get('features'):
            self.logger.warning("No features found in the response")
            return

        for location in data.get('features', []):
            if self.items_scraped >= self.max_items:
                self.logger.info(f"Reached the limit of {self.max_items} items. Stopping the spider.")
                return

            self.logger.debug(f"Processing location: {location}")

            properties = location.get('properties', {})
            geometry = location.get('geometry', {}).get('coordinates')

            addr_full = properties.get('addressLine1', '')
            city = properties.get('addressLine3', '')
            postcode = properties.get('postcode', '')
            state = properties.get('addressLine4', '')  # Assuming addressLine4 can be considered as state

            if not addr_full or not city or not postcode:
                self.logger.info(f"Skipping location with incomplete address details: {properties}")
                continue

            opening_hours = properties.get('restauranthours', {})
            formatted_opening_hours = {
                'Monday': opening_hours.get('hoursMonday', ''),
                'Tuesday': opening_hours.get('hoursTuesday', ''),
                'Wednesday': opening_hours.get('hoursWednesday', ''),
                'Thursday': opening_hours.get('hoursThursday', ''),
                'Friday': opening_hours.get('hoursFriday', ''),
                'Saturday': opening_hours.get('hoursSaturday', ''),
                'Sunday': opening_hours.get('hoursSunday', '')
            }

            mapped_attributes = {
                'ref': uuid.uuid4().hex,
                'name': properties.get('name', ''),
                'addr_full': f"{addr_full}, {city}, {state}",
                'city': city,
                'state': state,
                'postcode': postcode,
                'country': self.country_code,
                'lat': geometry[1] if geometry else None,
                'lon': geometry[0] if geometry else None,
                'phone': properties.get('telephone', ''),
                'email': '',
                'website': properties.get('website', ''),
                'chain_name': self.brand_name,
                'chain_id': self.spider_chain_id,
                'store_url': '',  # Assuming no store_url provided in properties
                'opening_hours': formatted_opening_hours,
                'extras': {
                    'opening_hours': formatted_opening_hours
                }
            }

            self.logger.info(f"Parsed location: {mapped_attributes}")
            yield GeojsonPointItem(
                ref=mapped_attributes['ref'],
                name=mapped_attributes['name'],
                addr_full=mapped_attributes['addr_full'],
                city=mapped_attributes['city'],
                state=mapped_attributes['state'],
                postcode=mapped_attributes['postcode'],
                country=mapped_attributes['country'],
                lat=mapped_attributes['lat'],
                lon=mapped_attributes['lon'],
                phone=mapped_attributes['phone'],
                email=mapped_attributes['email'],
                website=mapped_attributes['website'],
                chain_name=mapped_attributes['chain_name'],
                chain_id=mapped_attributes['chain_id'],
                extras=mapped_attributes['extras']
            )

            self.items_scraped += 1
