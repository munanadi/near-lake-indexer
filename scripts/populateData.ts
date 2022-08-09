import {
  TokenInfo,
  TokenListContainer,
  TokenListProvider,
} from '@tonic-foundation/token-list';
import {
  FormattedPool,
  InMemoryProvider,
  RefPool,
  StablePool,
  TonicMarket,
} from '@arbitoor/arbitoor-core';
import { Market as SpinMarket } from '@spinfi/core';
import axios from 'axios';
import { MainnetRpc } from 'near-workspaces';

// import data for creating images
// @ts-ignore
import unimportedData from './tokens-data.json';
import fs from 'fs';

// import data from local file instead of fetching always
// @ts-ignore
// import localData from './data/data';

// Used to pull data to a local file
import { createLogger, transports, format } from 'winston';

const logger = createLogger({
  format: format.printf(({ message }) => {
    return `${JSON.stringify(message)}`;
  }),
  transports: [
    new transports.File({
      filename: './tokens-data.json',
      level: 'info',
    }),
  ],
});

async function fetchTokensNotInList() {
  const notInListSet = new Set<string>();

  // If needed to pull data
  const tokens = await new TokenListProvider().resolve();
  const tokenList = tokens.filterByNearEnv('mainnet').getList();
  // const tokenList = localData.tokensMap as Array<TokenInfo>;

  // // console.log(tokenList);
  const tokensMap = tokenList.reduce((map, item) => {
    map.set(item.address, item);
    return map;
  }, new Map<string, TokenInfo>());

  let provider = new InMemoryProvider(MainnetRpc, tokensMap);
  await provider.fetchPools();

  const tonicMarkets = provider.getTonicMarkets();
  const spinMarkets = provider.getSpinMarkets();
  const refPools = provider.getRefPools();
  const refStablePools = provider.getRefStablePools();
  const jumboPools = provider.getJumboPools();
  const jumboStablePools = provider.getJumboStablePools();

  // Reading data from local file
  // const tonicMarkets = localData.tonicMarkets as Array<TonicMarket>;
  // const spinMarkets = localData.spinMarkets as Array<SpinMarket>;
  // const refPools = localData.refPools as Array<FormattedPool>;
  // const refStablePools = localData.refStablePools as Array<StablePool>;
  // const jumboPools = localData.jumboPools as Array<FormattedPool>;
  // const jumboStablePools = localData.jumboStablePools as Array<StablePool>;

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

  // Loop through all four dexes and keep saving token meta data of tokens not in list
  // Ref
  for (const [token0, token1] of refMarketTokenMap.values()) {
    if (!tokensMap.has(token0)) {
      notInListSet.add(token0);
    }
    if (!tokensMap.has(token1)) {
      notInListSet.add(token1);
    }
  }

  // Jumbo
  for (const [token0, token1] of jumboMarketTokenMap.values()) {
    if (!tokensMap.has(token0)) {
      notInListSet.add(token0);
    }
    if (!tokensMap.has(token1)) {
      notInListSet.add(token1);
    }
  }

  // Tonic
  for (const [token0, token1] of tonicMarketTokenMap.values()) {
    if (!tokensMap.has(token0)) {
      notInListSet.add(token0);
    }
    if (!tokensMap.has(token1)) {
      notInListSet.add(token1);
    }
  }

  // Spin
  for (const [token0, token1] of spinMarketTokenMap.values()) {
    if (!tokensMap.has(token0)) {
      notInListSet.add(token0);
    }
    if (!tokensMap.has(token1)) {
      notInListSet.add(token1);
    }
  }

  console.log(
    Array.from(notInListSet).toString(),
    notInListSet.size,
    ' are not in the tonic list'
  );

  const tokenInfos: TokenInfo[] = [];
  const dupSet = notInListSet;

  for (const token of notInListSet) {
    try {
      const data = await axios.get(
        `https://near-enhanced-api-server-mainnet.onrender.com/nep141/metadata/${token}`
      );

      console.log(data?.data);

      const { name, spec, symbol, icon, reference, reference_hash, decimals } =
        data?.data.metadata;

      const tokenInfo: TokenInfo = {
        address: token,
        decimals,
        icon,
        name,
        nearEnv: 'mainnet',
        reference,
        reference_hash,
        spec,
        symbol,
        extensions: undefined,
        tags: undefined,
        logoURI: undefined,
      };

      tokenInfos.push(tokenInfo);
      dupSet.delete(token); // Remove from set since added to list
    } catch (e) {
      console.log(`Couldn't fetch for token ${token}`);
      // TODO: Debug and check what's wrong?
    }
  }

  const tMap = tokenInfos;

  logger.info(tMap);

  console.log(dupSet, dupSet.size, ' number of tokens were not added');
}

// Loops through data and stores images in jpeg instead of base 64 format
async function fetchImages() {
  const tokensList = unimportedData;

  for (const token of tokensList) {
    const { icon, name } = token;

    if (!icon) return;

    const base64Data = icon?.replace(/^data:image\/jpeg;base64,/, '');

    fs.writeFile(`./images/${name}.jpeg`, base64Data, 'base64', (err) => {
      console.log(err);
    });
  }
}

(async function () {
  await fetchImages();
})();
