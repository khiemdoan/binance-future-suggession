
__author__ = 'Khiem Doan'
__github__ = 'https://github.com/khiemdoan'
__email__ = 'doankhiem.crazy@gmail.com'

import asyncio
import time

import httpx
import pandas as pd
from tqdm.asyncio import tqdm_asyncio

from constants import Interval


async def get_symbols() -> list[str]:
    resp = httpx.get('https://fapi.binance.com/fapi/v1/exchangeInfo')
    info = resp.json()
    symbols: list[str] = [row['symbol'] for row in info['symbols'] if row['status'] == 'TRADING']
    symbols = [s for s in symbols if s.endswith('BUSD') or s.endswith('USDT')]
    return symbols


async def get_klines(client: httpx.AsyncClient, symbol: str, interval: Interval):
    params = {
        'symbol': symbol,
        'interval': interval.value,
    }
    resp = await client.get('https://fapi.binance.com/fapi/v1/klines', params=params)
    return resp.json()


async def download_klines(symbols: list[str]):
    n = 2*len(symbols)
    limits = httpx.Limits(max_connections=n, max_keepalive_connections=n)

    async with httpx.AsyncClient(limits=limits) as client:
        tasks = [
            asyncio.ensure_future(get_klines(client, symbol, Interval.DAY_1))
            for symbol in symbols
        ]
        data_1_day = await asyncio.gather(*tasks)
        await asyncio.sleep(1)
        tasks = [
            asyncio.ensure_future(get_klines(client, symbol, Interval.HOUR_1))
            for symbol in symbols
        ]
        data_1_hour = await tqdm_asyncio.gather(*tasks)
    return data_1_day, data_1_hour


async def main() -> None:
    symbols = await get_symbols()
    print(f'Check {len(symbols)} symbols')
    await download_klines(symbols)


if __name__ == '__main__':
    start = time.time()
    asyncio.run(main())
    end = time.time()
    print(f'Total running time: {end - start:.2f}s')
