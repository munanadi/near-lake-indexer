import { TokenInfo, TokenListProvider } from '@tonic-foundation/token-list';
import {
  FormattedPool,
  InMemoryProvider,
  RefPool,
  StablePool,
  TonicMarket,
} from '@arbitoor/arbitoor-core';
import { Market as SpinMarket } from '@spinfi/core';
import { Pool } from 'pg';
import axios from 'axios';
import { MainnetRpc } from 'near-workspaces';
import Big from 'big.js';

// import data from local file instead of fetching always
// @ts-ignore
import localData from './data/data';

// Used to pull data to a local file
// import { createLogger, transports } from 'winston';

// const logger = createLogger({
//   transports: [
//     new transports.File({
//       filename: './data/data.json',
//       level: 'info',
//     }),
//   ],
// });

const pool = new Pool({
  connectionString: 'postgres://postgres:root@localhost:5432/postgres',
});

(async () => {
  // If needed to pull data
  // const tokens = await new TokenListProvider().resolve();
  // const tokenList = tokens.filterByNearEnv('mainnet').getList();
  const tokenList = localData.tokensMap as Array<TokenInfo>;

  // // console.log(tokenList);
  const tokensMap = tokenList.reduce((map, item) => {
    map.set(item.address, item);
    return map;
  }, new Map<string, TokenInfo>());

  // let provider = new InMemoryProvider(MainnetRpc, tokensMap);
  // await provider.fetchPools();

  // const tonicMarkets = provider.getTonicMarkets();
  // const spinMarkets = provider.getSpinMarkets();
  // const refPools = provider.getRefPools();
  // const refStablePools = provider.getRefStablePools();
  // const jumboPools = provider.getJumboPools();
  // const jumboStablePools = provider.getJumboStablePools();

  // Reading data from local file
  const tonicMarkets = localData.tonicMarkets as Array<TonicMarket>;
  const spinMarkets = localData.spinMarkets as Array<SpinMarket>;
  const refPools = localData.refPools as Array<FormattedPool>;
  const refStablePools = localData.refStablePools as Array<StablePool>;
  const jumboPools = localData.jumboPools as Array<FormattedPool>;
  const jumboStablePools = localData.jumboStablePools as Array<StablePool>;

  // logger.info(tonicMarkets);

  const refMarketTokenMap = new Map<string, string[]>();

  for (const pool of refPools) {
    refMarketTokenMap.set(pool.id.toString(), pool.token_account_ids);
  }
  for (const pool of refStablePools) {
    refMarketTokenMap.set(pool.id.toString(), pool.token_account_ids);
  }

  const jumboMarketTokenMap = new Map<string, string[]>();

  for (const pool of jumboPools) {
    jumboMarketTokenMap.set(pool.id.toString(), pool.token_account_ids);
  }
  for (const pool of jumboStablePools) {
    jumboMarketTokenMap.set(pool.id.toString(), pool.token_account_ids);
  }

  const tonicMarketTokenMap = tonicMarkets.reduce((map, item) => {
    const baseToken =
      (item.base_token.token_type['type'] === 'ft'
        ? item.base_token.token_type['account_id']
        : item.base_token.token_type['type']) ?? '';
    const quoteToken =
      (item.quote_token.token_type['type'] === 'ft'
        ? item.quote_token.token_type['account_id']
        : item.quote_token.token_type['type']) ?? '';
    map.set(item.id, [baseToken, quoteToken]);
    return map;
  }, new Map<string, string[]>());

  const spinMarketTokenMap = spinMarkets.reduce((map, item) => {
    const baseToken = item.base.address;
    const quoteToken = item.quote.address;
    map.set(item.id, [baseToken, quoteToken]);
    return map;
  }, new Map<number, string[]>());

  // // fetch and store prices
  // await fetchTokenPrices();

  const dailyVolume: {
    [date: number]: string;
  } = {};

  // 1st July
  const startDate = new Date('07-01-2022');
  // Today
  const endDate = new Date('07-02-2022');

  let now = startDate;

  while (now.getTime() < endDate.getTime()) {
    // get until next day from start
    let tmr = new Date(now.getTime() + 1000 * 60 * 60 * 24);

    console.log(
      `Fetching data for the period ${now.toString()} to ${tmr.toString()}`
    );

    const result = await pool.query(
      'select sum(amount_in) as amount_in, sum(amount_out) as amount_out, pool_id, dex from arbitoor_txns where blocktime between $1 and $2 group by pool_id, dex',
      [now.getTime(), tmr.getTime()]
    );

    for (const row of result.rows) {
      console.log(row);

      let tokenIn = '';
      let tokenOut = '';

      switch (row.dex) {
        case 'v2.ref-finance.near': {
          tokenIn = refMarketTokenMap.get(row.pool_id.toString())?.[0] ?? '';
          tokenOut = refMarketTokenMap.get(row.pool_id.toString())?.[1] ?? '';
          break;
        }
        case 'v1.jumbo_exchange.near': {
          tokenIn = jumboMarketTokenMap.get(row.pool_id.toString())?.[0] ?? '';
          tokenOut = jumboMarketTokenMap.get(row.pool_id.toString())?.[1] ?? '';
          break;
        }
        case 'v1.orderbook.near': {
          tokenIn = tonicMarketTokenMap.get(row.pool_id.toString())?.[0] ?? '';
          tokenOut = tonicMarketTokenMap.get(row.pool_id.toString())?.[1] ?? '';
          break;
        }
        case 'spot.spin-fi.near': {
          tokenIn = spinMarketTokenMap.get(row.pool_id.toString())?.[0] ?? '';
          tokenOut = spinMarketTokenMap.get(row.pool_id.toString())?.[1] ?? '';
          break;
        }
      }

      const token0Decimals = tokensMap.get(tokenIn)?.decimals ?? 1;
      const token1Decimals = tokensMap.get(tokenOut)?.decimals ?? 1;

      console.log(
        row.amount_in,
        new Big(row.amount_in).mul(new Big(10).pow(-token0Decimals)).toString(),
        token0Decimals,
        tokenIn,
        row.amount_out,
        new Big(row.amount_out)
          .mul(new Big(10).pow(-token1Decimals))
          .toString(),
        token1Decimals,
        tokenOut
      );
    }

    now = tmr;
  }
})();

// Fetch token prices every 30 mins
let tokenPrices = new Map<string, number>();
let cachedTokenPrices = new Map<string, number>();

async function fetchTokenPrices() {
  console.log('Fetching token prices');
  let data: any = null;
  try {
    data = (await axios.get('https://indexer.ref.finance/list-token-price'))
      .data;
  } catch (e) {
    console.log('Err fetching prices,', e);
  }

  if (!data) {
    tokenPrices = cachedTokenPrices;
  }

  console.log('Fetched token prices');

  for (const add of Object.keys(data)) {
    tokenPrices.set(add, +data[add].price);
  }
}
