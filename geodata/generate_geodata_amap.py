import requests
import time
import os
import sys
import csv
from utils import logger, GEODATA_HEADER, load_geo_data
from requests.adapters import HTTPAdapter, Retry
import argparse
import eviltransform
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

AMAP_API_KEY = os.environ["AMAP_API_KEY"]
AMAP_QPS = int(os.environ.get("AMAP_QPS", "3"))
AMAP_BATCH_SIZE = int(os.environ.get("AMAP_BATCH_SIZE", "20"))

GEONAME_DATA_FILE = args.data_file

s = requests.Session()

retries = Retry(total=5, backoff_factor=0.1, status_forcelist=[403, 500, 502, 503, 504])

s.mount("https://", HTTPAdapter(max_retries=retries))


@sleep_and_retry
@limits(calls=AMAP_QPS, period=1)
def get_loc_from_amap(loc_list):
    loc = "|".join([f"{loc['lon_gcj']},{loc['lat_gcj']}" for loc in loc_list])
    r = f"https://restapi.amap.com/v3/geocode/regeo?output=json&location={loc}&key={AMAP_API_KEY}&radius=1000&extensions=all&batch=true"
    # print(r)
    resp = s.get(r)
    # time.sleep(1.02 / AMAP_QPS)
    return resp.json()


def process_file(file_path, country_code, output_file, existing_data={}):
    batch_size = AMAP_BATCH_SIZE
    coordinates_batch = []

    with open(file_path, "r", encoding="utf-8") as file:
        for line in file:
            fields = line.strip().split("\t")

            # 检查国家码
            if len(fields) > 17 and fields[8] == country_code:
                # print(hello)
                loc = {"lon": str(fields[5]), "lat": str(fields[4])}
                if (loc["lon"], loc["lat"]) not in existing_data:
                    lat_gcj, lon_gcj = eviltransform.wgs2gcj(
                        float(fields[4]), float(fields[5])
                    )
                    loc["lat_gcj"] = str(lat_gcj)
                    loc["lon_gcj"] = str(lon_gcj)
                    coordinates_batch.append(loc)

                # 如果达到批量大小，执行查询
                if len(coordinates_batch) == batch_size:
                    query_and_store(coordinates_batch, output_file)
                    coordinates_batch = []

        # 处理剩余的坐标
        if coordinates_batch:
            query_and_store(coordinates_batch, output_file)


def query_and_store_one(coordinate, output_file):
    response = get_loc_from_amap([coordinate])

    if (
        response.get("status") != "1"
        or "regeocodes" not in response
        or len(response["regeocodes"]) != 1
    ):
        x = {
            "latitude": coordinate["lat"],
            "longitude": coordinate["lon"],
            "country": "",
            "admin_1": "",
            "admin_2": "",
            "admin_3": "",
            "admin_4": "",
        }
    else:
        address = response["regeocodes"][0]["addressComponent"]
        x = {
            "latitude": coordinate["lat"],
            "longitude": coordinate["lon"],
            "country": address["country"],
            "admin_1": address["province"],
            "admin_2": (address["city"] if address["city"] else address["province"]),
            "admin_3": (
                address["district"] if address["district"] else address["city"]
            ),
            "admin_4": address["township"],
        }

    for k in x:
        if x[k] == []:
            x[k] = ""

    with open(output_file, mode="a", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=GEODATA_HEADER)

        # 如果文件为空，写入表头
        if file.tell() == 0:
            writer.writeheader()

        # 写入数据
        writer.writerows([x])


def query_and_store(coordinates_batch, output_file):
    response = get_loc_from_amap(coordinates_batch)

    if response.get("status") == "1":
        regeocode_data = response["regeocodes"]
        if len(coordinates_batch) != len(regeocode_data):
            logger.warning(
                f"获得坐标数量不同，请求{len(coordinates_batch)}，返回{len(regeocode_data)}"
            )
            for coord in coordinates_batch:
                query_and_store_one(coord, output_file)
            return
        results = [
            {
                "latitude": coord["lat"],
                "longitude": coord["lon"],
                "address_component": data["addressComponent"],
            }
            for coord, data in zip(coordinates_batch, regeocode_data)
        ]

        # 保存到文件
        if results:
            records = []
            for item in results:
                address = item["address_component"]
                x = {
                    "latitude": item["latitude"],
                    "longitude": item["longitude"],
                    "country": address["country"],
                    "admin_1": address["province"],
                    "admin_2": (
                        address["city"] if address["city"] else address["province"]
                    ),
                    "admin_3": (
                        address["district"] if address["district"] else address["city"]
                    ),
                    "admin_4": address["township"],
                }
                for k in x:
                    if x[k] == []:
                        x[k] = ""
                records.append(x)

            with open(output_file, mode="a", newline="", encoding="utf-8") as file:
                writer = csv.DictWriter(file, fieldnames=GEODATA_HEADER)

                # 如果文件为空，写入表头
                if file.tell() == 0:
                    writer.writeheader()

                # 写入数据
                writer.writerows(records)
                logger.info(f"新增 {len(records)} 条记录")

    else:
        # 打印失败的坐标
        logger.error(f"查询失败，坐标: {coordinates_batch}，响应：{response}")
        for coord in coordinates_batch:
            query_and_store_one(coord, output_file)


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
