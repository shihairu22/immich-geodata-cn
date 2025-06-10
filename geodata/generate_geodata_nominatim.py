import requests
import time
import os
import sys
import csv
from utils import logger, GEODATA_HEADER, load_geo_data
from requests.adapters import HTTPAdapter, Retry
import argparse
from ratelimit import limits, sleep_and_retry

parser = argparse.ArgumentParser()

parser.add_argument(
    "--data-file",
    type=str,
    default="./geoname_data/cities500.txt",
    help="input geodata file",
)

parser.add_argument(
    "--country-code", type=str, default="CN", help="country code to be generated"
)
# 解析参数
args = parser.parse_args()

NOMINATIM_QPS = int(os.environ.get("NOMINATIM_QPS", "1"))

GEONAME_DATA_FILE = args.data_file

s = requests.Session()

retries = Retry(total=5, backoff_factor=0.1, status_forcelist=[403, 500, 502, 503, 504])

s.mount("https://", HTTPAdapter(max_retries=retries))
s.headers.update(
    {
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/137.0.0.0 Safari/537.36",
        "Referer": "https://jfioasdfo.com",
    }
)


@sleep_and_retry
@limits(calls=NOMINATIM_QPS, period=1)
def get_loc_from_locationiq(lat, lon):
    url = "https://nominatim.openstreetmap.org/reverse"

    params = {
        "lat": lat,
        "lon": lon,
        "format": "geocodejson",
        "accept-language": "zh-CN,zh;q=0.9,en;q=0.8",
    }

    try:
        response = s.get(url, params=params)
        # time.sleep(1.01 / NOMINATIM_QPS)
        if response.status_code == 200:
            return response.json()
    except Exception as e:
        print(e)
        logger.error(f"{lat},{lon} failed to get location")
        pass
    return None


def process_file(file_path, country_code, output_file, existing_data={}):
    # 打开并读取文件
    with open(file_path, "r", encoding="utf-8") as file:
        for line in file:
            fields = line.strip().split("\t")

            # 检查国家码
            if len(fields) > 17 and fields[8] == country_code:
                loc = {"lon": str(fields[5]), "lat": str(fields[4])}
                if (loc["lon"], loc["lat"]) in existing_data:
                    continue
                query_and_store(loc, output_file)


def query_and_store(coordinate, output_file):
    response = get_loc_from_locationiq(coordinate["lat"], coordinate["lon"])

    if response and "features" in response and len(response["features"]) > 0:
        address = response["features"][0]["properties"]["geocoding"]
        admin = address["admin"]
        record = {
            "latitude": coordinate["lat"],
            "longitude": coordinate["lon"],
            "country": address["country"],
        }
        sorted_items = sorted(
            admin.items(), key=lambda x: int(x[0].replace("level", ""))
        )
        top_four = [name for _, name in sorted_items[:4]]

        # 不足 4 个用空字符串补齐
        while len(top_four) < 4:
            top_four.append("")

        for i in range(4):
            record[f"admin_{i+1}"] = top_four[i]

    else:
        # 打印失败的坐标
        logger.error(f"查询失败，坐标: {coordinate}")
        record = {
            "latitude": coordinate["lat"],
            "longitude": coordinate["lon"],
            "country": "",
            "admin_1": "",
            "admin_2": "",
            "admin_3": "",
            "admin_4": "",
        }

    with open(output_file, mode="a", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=GEODATA_HEADER)

        # 如果文件为空，写入表头
        if file.tell() == 0:
            writer.writeheader()

        # 写入数据
        writer.writerows([record])


def main():
    country_code = args.country_code
    logger.info(f"待处理国家码：{country_code}")
    if not os.path.isfile(GEONAME_DATA_FILE):
        raise Exception(f"文件 '{GEONAME_DATA_FILE}' 不存在，请下载后重试。")
    output_file = os.path.join("data", f"{country_code}.csv")
    existing_data = load_geo_data(output_file)
    process_file(
        GEONAME_DATA_FILE, country_code, output_file, existing_data=existing_data
    )


if __name__ == "__main__":
    main()
