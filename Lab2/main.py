import asyncio
import aiohttp
import requests, zipfile, io
import os

download_uris = [
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2018_Q4.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q1.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q2.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q3.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q4.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2020_Q1.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2220_Q1.zip",
]

def makeDir():
    if not os.path.exists('downloads'):
        # Create the folder
        os.makedirs('downloads')
        print(f"Folder downloads created.")
    else:
        print(f"Folder downloads already exists.")

def download_sync(uri):
    try:
        filename = uri.split("/")[-1];
        print(f'Downloading {filename}')

        response = requests.get(uri, stream=True)
        response.raise_for_status()
        print(f"Downloaded {filename}")

        print(f'Extracting from {filename}')
        zip = zipfile.ZipFile(io.BytesIO(response.content))
        zip.extract(f'{filename.split(".")[0]}.csv', 'downloads/')
        print(f'Extracted {filename.split(".")[0]}.csv')
    except requests.exceptions.HTTPError as errh:
        print(f"HTTP Error: {errh} for {uri}")
    except requests.exceptions.ConnectionError as errc:
        print(f"Error Connecting: {errc} for {uri}")
    except requests.exceptions.Timeout as errt:
        print(f"Timeout Error: {errt} for {uri}")
    except requests.exceptions.RequestException as err:
        print(f"Request Exception: {err} for {uri}")

async def download_async():
    async with aiohttp.ClientSession() as session:
            for uri in download_uris:
                try:
                    await download_file_async(uri, session)
                except requests.exceptions.HTTPError as errh:
                    print(f"HTTP Error: {errh} for {uri}")
                    continue
                except requests.exceptions.ConnectionError as errc:
                    print(f"Error Connecting: {errc} for {uri}")
                    continue
                except requests.exceptions.Timeout as errt:
                    print(f"Timeout Error: {errt} for {uri}")
                    continue
                except requests.exceptions.RequestException as err:
                    print(f"Request Exception: {err} for {uri}")
                    continue

async def download_file_async(uri, session):
    async with session.get(uri) as response:
        content = await response.read()
        filename = uri.split("/")[-1];
        print(f"Async downloaded {filename}")

        print(f'Async extracting from {filename}')
        zip = zipfile.ZipFile(io.BytesIO(response.content))
        zip.extract(f'{filename.split(".")[0]}.csv', 'downloads')
        print(f'Async extracted {filename.split(".")[0]}.csv')

async def main():
    makeDir()

    #sync download
    for uri in download_uris:
        download_sync(uri)

    #async download
    #await download_async()

    pass


if __name__ == "__main__":
    asyncio.run(main())
