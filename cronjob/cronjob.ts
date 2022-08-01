import { TokenInfo, TokenListProvider } from '@tonic-foundation/token-list';
import {
  FormattedPool,
  InMemoryProvider,
  RefPool,
  TonicMarket,
} from '@arbitoor/arbitoor-core';
import { Market as SpinMarket } from '@spinfi/core';
import { Pool } from 'pg';
import axios from 'axios';
import { MainnetRpc } from 'near-workspaces';
import Big from 'big.js';

const pool = new Pool({
  connectionString: 'postgres://postgres:root@localhost:5432/postgres',
});

// To fetch token lists
let provider: InMemoryProvider | null;
let tokenMap: Map<string, TokenInfo>;
let tonicMarketMap: Map<string, TonicMarket>;
let spinMarketMap: Map<number, SpinMarket>;

(async () => {
  /**
   * Setup - fetch tonic markets
   */

  console.log('Setting up');
  const tokens = await new TokenListProvider().resolve();
  const tokenList = tokens.filterByNearEnv('mainnet').getList();
  // console.log(tokenList);
  tokenMap = tokenList.reduce((map, item) => {
    map.set(item.address, item);
    return map;
  }, new Map<string, TokenInfo>());

  provider = new InMemoryProvider(MainnetRpc, tokenMap);

  await provider.fetchPools();

  // const tonicMarkets = provider.getTonicMarkets();
  // const spinMarkets = provider.getSpinMarkets();
  const refPools = provider.getRefPools();
  const refStablePools = provider.getRefStablePools();

  const refMarketMap = new Map<string, string[]>();

  for (const pool of refPools) {
    refMarketMap.set(pool.id.toString(), pool.token_account_ids);
  }
  for (const pool of refStablePools) {
    refMarketMap.set(pool.id.toString(), pool.token_account_ids);
  }

  // console.table(refMarketMap);

  // tonicMarketMap = tonicMarkets.reduce((map, item) => {
  //   map.set(item.id, item);
  //   return map;
  // }, new Map<string, TonicMarket>());

  // spinMarketMap = spinMarkets.reduce((map, item) => {
  //   map.set(item.id, item);
  //   return map;
  // }, new Map<number, SpinMarket>());

  // // fetch and store prices
  // await fetchTokenPrices();

  // await startStream(lakeConfig, handleStreamerMessage);

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
          tokenIn = refMarketMap.get(row.pool_id.toString())?.[0] ?? '';
          tokenOut = refMarketMap.get(row.pool_id.toString())?.[1] ?? '';
          break;
        }
      }

      const token0Decimals = tokenMap.get(tokenIn)?.decimals ?? 1;
      const token1Decimals = tokenMap.get(tokenOut)?.decimals ?? 1;

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

setInterval(async () => {
  // await fetchTokenPrices();
}, 1000 * 60 * 30);

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
