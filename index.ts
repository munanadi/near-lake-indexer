import { startStream, types } from 'near-lake-framework';
import { createLogger, transports, format } from 'winston';
// @ts-ignore
import CSV from 'winston-csv-format';
import { Big } from 'big.js';
import { TokenInfo, TokenListProvider } from '@tonic-foundation/token-list';
import { MainnetRpc } from 'near-workspaces';
import {
  FormattedPool,
  InMemoryProvider,
  StablePool,
  TonicMarket,
} from '@arbitoor/arbitoor-core';
import { Market as SpinMarket } from '@spinfi/core';
import { Pool } from 'pg';
import axios from 'axios';

// @ts-ignore
import localData from './cronjob/data/data.json';

const startingBlock = +process.argv.slice(2)[0];

// To keep track of toekns that are not in the tonic list
const tokenMetadataLogger = createLogger({
  level: 'info',
  format: CSV(['token_add', 'receipt_id', 'added_to_list'], {
    delimiter: ',',
  }),
  transports: [
    new transports.File({
      filename: './logs/tokens.txt',
      level: 'info',
    }),
  ],
});

const txnCsvLogger = createLogger({
  level: 'info',
  format: CSV(
    [
      'receipt_id',
      'block_height',
      'block_time',
      'pool_id',
      'sender',
      'amount_in',
      'amount_out',
      'token_in',
      'token_out',
      'success',
      'dex',
    ],
    { delimiter: ',' }
  ),
  transports: [
    new transports.File({
      filename: './logs/not_written.txt',
      level: 'info',
      maxFiles: 4000000,
    }),
  ],
});

const readPool = new Pool({
  connectionString:
    'postgres://public_readonly:nearprotocol@mainnet.db.explorer.indexer.near.dev/mainnet_explorer',
});

const pool = new Pool({
  connectionString: 'postgres://ubuntu:root@localhost:5433/ubuntu',
  // connectionString: 'postgres://ubuntu:root@localhost:5432/ubuntu',
});

const lakeConfig: types.LakeConfig = {
  s3BucketName: 'near-lake-data-mainnet',
  s3RegionName: 'eu-central-1',
  // startBlockHeight: startingBlock ?? 69253110, // Jumbo
  // startBlockHeight: 70223685, // Tonic
  // startBlockHeight: 69229361, // Ref
  // startBlockHeight: 69328535, // Spin
  // startBlockHeight: 70429294, // Two Ref together , 294, 335
  startBlockHeight: startingBlock ?? 68893936, // start of memo 01/07
};

const receiptsSetToTrack = new Set<string>();

const masterReceiptMap = new Map<string, string>();

const didTxnWork: { [receipId: string]: boolean } = {};

const resultRows = new Map<
  string,
  {
    blockTime: number;
    blockHeight: number;
    sender: string;
    success: boolean;
    dex: string;
    swaps: {
      pool_id: string;
      amount_in: string;
      amount_out: string;
      token_in: string;
      token_out: string;
    }[];
  }
>();

async function handleStreamerMessage(
  streamerMessage: types.StreamerMessage
): Promise<void> {
  const block = streamerMessage.block;
  const shards = streamerMessage.shards;

  console.log(
    `Block #${block.header.height} ${new Date(
      +block.header.timestampNanosec / 1000000
    ).toString()} Shards: ${shards.length}`
  );

  for (const shard of shards) {
    if (shard.receiptExecutionOutcomes) {
      const receiptExecutionOutcomes = shard.receiptExecutionOutcomes;

      for (const receiptExecutionOutcome of receiptExecutionOutcomes) {
        const { executionOutcome, receipt: ExeReceipt } =
          receiptExecutionOutcome;

        const {
          id,
          outcome: { logs, receiptIds, executorId },
        } = executionOutcome;

        const status: any = executionOutcome.outcome.status;

        if (!ExeReceipt) {
          console.log('Receipt not found');
          process.exit();
        }

        const { receiptId, predecessorId } = ExeReceipt;
        const receipt: any = ExeReceipt.receipt;

        if (receiptsSetToTrack.has(receiptId)) {
          if (receipt['Action'] && receipt['Action'].actions) {
            for (const action of receipt['Action'].actions) {
              if (!action['FunctionCall']) {
                continue;
              }

              const methodName = action['FunctionCall'].methodName;

              const originalReceipt = masterReceiptMap.get(receiptId);

              // If original Receipt not found,
              // prev txns tracking got missde
              if (!originalReceipt) {
                console.log('original receipt not found');
                process.exit();
              }

              const returnedJson = takeActionsAndReturnArgs(action);

              if (
                ['callback_ft_on_transfer', 'ft_on_transfer'].includes(
                  methodName
                )
              ) {
                // For AMM's actions has different structure
                if (
                  ['v2.ref-finance.near', 'v1.jumbo_exchange.near'].includes(
                    executorId
                  )
                ) {
                  const msgActions = JSON.parse(returnedJson['msg'])['actions'];

                  for (const action of msgActions) {
                    // Without this jumbo will add this action twice
                    if (methodName !== 'callback_ft_on_transfer') {
                      const newSwapsArr =
                        resultRows.get(originalReceipt)?.swaps ?? [];
                      const oldData = resultRows.get(originalReceipt)!;

                      newSwapsArr.push({
                        pool_id: action.pool_id,
                        amount_in: action.amount_in,
                        amount_out: '0',
                        token_in: action.token_in,
                        token_out: action.token_out ?? '0',
                      });

                      resultRows.set(originalReceipt, {
                        ...oldData,
                        swaps: newSwapsArr,
                      });

                      // Will add to set to fetch meta data
                      if (!tokenSet.has(action.token_in)) {
                        notInTokenSet.add(action.token_in);
                      }
                      if (!tokenSet.has(action.token_out)) {
                        notInTokenSet.add(action.token_out);
                      }
                    }
                  }

                  if (logs.length) {
                    let i = 0;

                    for (const log of logs) {
                      const logSplit = log.split(' ');
                      if (logSplit[0] === 'Swapped') {
                        // Swapped logs here
                        // ex. Swapped 2864400000000000000 usn for 2863230 dac17f958d2ee523a2206206994597c13d831ec7.factory.bridge.near
                        const amount_in = logSplit[1];
                        const token_in = logSplit[2];
                        const amount_out = logSplit[4];
                        const token_out = logSplit[5].split(',')[0];

                        // console.log(log);
                        // Will add to set to fetch meta data
                        if (!tokenSet.has(token_in)) {
                          notInTokenSet.add(token_in);
                        }
                        if (!tokenSet.has(token_out)) {
                          notInTokenSet.add(token_out);
                        }

                        // Add to result row
                        const newSwapsArr =
                          resultRows.get(originalReceipt)?.swaps ?? [];
                        const oldData = resultRows.get(originalReceipt)!;

                        newSwapsArr[i] = {
                          amount_in,
                          amount_out,
                          token_in,
                          token_out,
                          pool_id: oldData['swaps'][i].pool_id,
                        };

                        resultRows.set(originalReceipt, {
                          ...oldData,
                          swaps: newSwapsArr,
                        });

                        i++;
                      }
                    }
                  }
                } else if (
                  ['v1.orderbook.near', 'spot.spin-fi.near'].includes(
                    executorId
                  )
                ) {
                  if (methodName === 'ft_on_transfer') {
                    const amount_in = returnedJson['amount'];

                    let pool_id = '';

                    let base_token = '';
                    let quote_token = '';

                    switch (executorId) {
                      case 'v1.orderbook.near': {
                        // Only one pool hops for now
                        const params = JSON.parse(returnedJson['msg'])[
                          'params'
                        ];
                        pool_id = params[0]['market_id'];

                        base_token =
                          (tonicMarketMap.get(pool_id)?.base_token as any)
                            .token_type['type'] === 'ft'
                            ? (tonicMarketMap.get(pool_id)?.base_token as any)
                                .token_type['account_id']
                            : (tonicMarketMap.get(pool_id)?.base_token as any)
                                .type;
                        quote_token =
                          (tonicMarketMap.get(pool_id)?.quote_token as any)
                            .token_type['type'] === 'ft'
                            ? (tonicMarketMap.get(pool_id)?.quote_token as any)
                                .token_type['account_id']
                            : (tonicMarketMap.get(pool_id)?.quote_token as any)
                                .type;

                        break;
                      }
                      case 'spot.spin-fi.near': {
                        const params = JSON.parse(returnedJson['msg']);
                        pool_id = params['market_id'];

                        base_token =
                          spinMarketMap.get(+pool_id)?.base.address ?? '';
                        quote_token =
                          spinMarketMap.get(+pool_id)?.quote.address ?? '';
                        break;
                      }
                      default: {
                        console.log('-!!!!!!!!!!');
                      }
                    }

                    // console.log(JSON.stringify(tonicMarketMap[pool_id], null, 2));

                    const newSwapsArr =
                      resultRows.get(originalReceipt)?.swaps ?? [];
                    const oldData = resultRows.get(originalReceipt)!;

                    newSwapsArr.push({
                      pool_id,
                      amount_in,
                      amount_out: '0',
                      token_in: base_token,
                      token_out: quote_token,
                    });

                    // Will add to set to fetch meta data
                    if (!tokenSet.has(base_token)) {
                      notInTokenSet.add(base_token);
                    }
                    if (!tokenSet.has(quote_token)) {
                      notInTokenSet.add(quote_token);
                    }

                    resultRows.set(originalReceipt, {
                      ...oldData,
                      swaps: newSwapsArr,
                    });

                    // Add the first receipt to track ft_trasnfer for output

                    receiptIds.forEach((r) => {
                      receiptsSetToTrack.add(r); // Not sure if always the second one
                      masterReceiptMap.set(r, originalReceipt);
                    });
                  }
                }
              } else if (methodName === 'ft_transfer') {
                if (
                  predecessorId === 'v1.orderbook.near' ||
                  predecessorId === 'spot.spin-fi.near'
                ) {
                  const amount_out = returnedJson['amount'] ?? '0';

                  const oldSwapData =
                    resultRows.get(originalReceipt)?.swaps[0]!;

                  const oldData = resultRows.get(originalReceipt)!;

                  resultRows.set(originalReceipt, {
                    ...oldData,
                    swaps: [
                      {
                        ...oldSwapData,
                        amount_out,
                      },
                    ],
                  });

                  masterReceiptMap.delete(receiptId);
                }
              } else if (methodName === 'ft_resolve_transfer') {
                // Success txn check

                const success = status['SuccessValue'] ? true : false;

                if (success) {
                  didTxnWork[originalReceipt] = true;

                  const oldData = resultRows.get(originalReceipt)!;

                  resultRows.set(originalReceipt, {
                    ...oldData,
                    success: true,
                  });
                }
              }
            }
          }

          // Add Status receipt ids to track - Needed for jumbo
          // Add this to the master map to track if txn was a success and map it back to the original id to track
          if (status['SuccessReceiptId']) {
            receiptsSetToTrack.add(status['SuccessReceiptId']);

            masterReceiptMap.set(
              status['SuccessReceiptId'],
              masterReceiptMap.get(id) ?? ''
            );
          }

          // Delete receipt id after tracking
          receiptsSetToTrack.delete(receiptId);

          // Remove from map too
          masterReceiptMap.delete(id);
        }

        // Track the memo and initia the tracking here
        if (receipt['Action'] && receipt['Action'].actions) {
          const receiptActions = receipt['Action'].actions;

          for (const action of receiptActions) {
            if (action['FunctionCall']) {
              if (action['FunctionCall']) {
                if (action['FunctionCall'].methodName === 'ft_transfer_call') {
                  const args = action['FunctionCall'].args;

                  try {
                    const decodedArgs = Buffer.from(args.toString(), 'base64');
                    const parsedJSONArgs = JSON.parse(decodedArgs.toString());
                    const memo = parsedJSONArgs['memo'] ?? '';
                    const dex = parsedJSONArgs['receiver_id'] ?? '';

                    if (memo === 'arbitoor') {
                      // Add receipt ids to track
                      receiptIds.forEach((receipt) => {
                        masterReceiptMap.set(receipt, id);
                        receiptsSetToTrack.add(receipt);
                      });

                      // For succes tracking
                      didTxnWork[id] = false;

                      resultRows.set(id, {
                        dex,
                        blockTime: parseInt(
                          new Big(block.header.timestampNanosec)
                            .mul(new Big(10).pow(-9))
                            .mul(new Big(1000))
                            .toString()
                        ),
                        blockHeight: block.header.height,
                        sender: action.signerId ?? predecessorId ?? '',
                        success: false,
                        swaps: [],
                      });
                    }
                  } catch (e) {
                    // TODO: handle better, exit to investigate what went wrong maybe?
                    console.log('FAILED', e);
                  }
                }
              }
            }
          }
        }
      }
    }
  }

  // WHAT IF WE FETCH TOKENS METADATA HERE AND STORE IN TOKEN MAP?
  for (const token of notInTokenSet) {
    console.log(`Fetching metadata for ${token}`);
    const data = await axios.get(
      `https://near-enhanced-api-server-mainnet.onrender.com/nep141/metadata/${token}`
    );
    if (data?.data) {
      console.log(`Fetced ${data?.data.toString()}`);
      tokenMap.set(token, data?.data);
      notInTokenSet.delete(token);
      tokenMetadataLogger.info('info', {
        token_add: token,
        receipt_id: '----',
        added_to_list: true,
      });
    } else {
      tokenMetadataLogger.info('info', {
        token_add: token,
        receipt_id: 'XXX',
        added_to_list: false,
      });
    }
  }

  if (receiptsSetToTrack.size) {
    console.log(
      `Receipt Ids to track are ${Array.from(receiptsSetToTrack).map((r) =>
        r.toString()
      )}`
    );
  }

  for (const result of resultRows) {
    const hash = result[0];
    const { blockTime, dex, sender, swaps } = result[1];

    console.log(
      `- ${hash} ${new Date(blockTime)
        .toString()
        .slice(0, 24)} ${dex} ${sender}`
    );

    for (const swap of swaps) {
      const { pool_id, token_in, amount_in, token_out, amount_out } = swap;

      let token0Decimals = tokenMap.get(token_in)?.decimals ?? 1;
      let token1Decimals = tokenMap.get(token_out)?.decimals ?? 1;

      // STOOPID HACK TO GET AROUND NOT BEING ABLE TO * (10 ** -24)
      if (token0Decimals >= 15 || token1Decimals >= 15) {
        const rest0 = token0Decimals - 15;
        const rest1 = token1Decimals - 15;

        console.log(
          `\t ${pool_id} ${new Big(
            new Big(amount_in).mul(new Big(10).pow(-15)).toString()
          )
            .mul(new Big(10).pow(-rest0))
            .toString()} ${token_in} -> ${new Big(
            new Big(amount_out).mul(new Big(10).pow(-15)).toString()
          )
            .mul(new Big(10).pow(-rest1))
            .toString()} ${token_out}`
        );
      } else {
        console.log(
          `\t ${pool_id} ${new Big(amount_in)
            .mul(new Big(10).pow(-token0Decimals))
            .toString()} ${token_in} -> ${new Big(amount_out)
            .mul(new Big(10).pow(-token1Decimals))
            .toString()} ${token_out}`
        );
      }
    }
  }

  const swapRows: any[] = [];

  if (Object.keys(didTxnWork)) {
    // Add successful transactions into DB

    let query = `INSERT INTO arbitoor_txns (
      receipt_id, 
      block_height, 
      blocktime,
      dex,
      sender,
      success,
      amount_in,
      amount_in_d,
      amount_in$,
      amount_in$_d,
      amount_out,
      amount_out_d,
      amount_out$,
      amount_out$_d,
      pool_id,
      token_in,
      token_out ) VALUES `;

    for (const result of resultRows) {
      const txn = result[0];
      const { blockHeight, blockTime, dex, sender, success, swaps } = result[1];

      // Add only succeded swaps
      if (!success) {
        continue;
      }

      for (const swap of swaps) {
        // console.log(swap);

        const { amount_in, amount_out, pool_id, token_in, token_out } = swap;

        // token decimals
        const token0Decimal = tokenMap.get(token_in)?.decimals ?? 1;
        const token1Decimal = tokenMap.get(token_out)?.decimals ?? 1;

        const amount0InDecimals =
          token0Decimal >= 15
            ? new Big(
                new Big(amount_in.toString()).mul(new Big(10).pow(-15))
              ).mul(new Big(10).pow(-(token0Decimal - 15)))
            : new Big(amount_in.toString()).mul(
                new Big(10).pow(-token0Decimal)
              );
        const amount1InDecimals =
          token1Decimal >= 15
            ? new Big(
                new Big(amount_out.toString()).mul(new Big(10).pow(-15))
              ).mul(new Big(10).pow(-(token1Decimal - 15)))
            : new Big(amount_out.toString()).mul(
                new Big(10).pow(-token1Decimal)
              );

        // If token prices are present, then multiply it with the amount
        const final_amount_in = new Big(amount_in.toString())
          .mul(new Big(tokenPrices.get(token_in) ?? '1'))
          .toString();
        const final_amount_out = new Big(amount_out.toString())
          .mul(new Big(tokenPrices.get(token_out) ?? '1'))
          .toString();

        const finalAmountInDecimals =
          token0Decimal >= 15
            ? new Big(
                new Big(final_amount_in.toString()).mul(new Big(10).pow(-15))
              ).mul(new Big(10).pow(-(token0Decimal - 15)))
            : new Big(final_amount_in.toString()).mul(
                new Big(10).pow(-token0Decimal)
              );
        const finalAmountOutDecimals =
          token0Decimal >= 15
            ? new Big(
                new Big(final_amount_out.toString()).mul(new Big(10).pow(-15))
              ).mul(new Big(10).pow(-(token1Decimal - 15)))
            : new Big(final_amount_out.toString()).mul(
                new Big(10).pow(-token1Decimal)
              );

        swapRows.push({
          txn,
          blockHeight,
          blockTime,
          dex,
          sender,
          success,
          amount_in,
          amount_in_d: amount0InDecimals,
          amount_in$: final_amount_in,
          amount_in$_d: finalAmountInDecimals,
          amount_out,
          amount_out_d: amount1InDecimals,
          amount_out$: final_amount_out,
          amount_out$_d: finalAmountOutDecimals,
          pool_id,
          token_in,
          token_out,
        });

        // remove from resultRows
      }
      resultRows.delete(txn);
    }

    // console.log(swapRows);

    if (swapRows.length) {
      console.log('Adding Txn into postgres');

      query += `${swapRows.map(
        ({
          txn,
          blockHeight,
          blockTime,
          dex,
          sender,
          success,
          amount_in,
          amount_in_d,
          amount_in$,
          amount_in$_d,
          amount_out,
          amount_out_d,
          amount_out$,
          amount_out$_d,
          pool_id,
          token_in,
          token_out,
        }) =>
          `('${txn}', ${blockHeight}, ${blockTime}, '${dex}', '${sender}', '${success}', ${amount_in}, ${amount_in_d}, ${amount_in$}, ${amount_in$_d}, '${amount_out}', '${amount_out_d}', '${amount_out$}', '${amount_out$_d}', '${pool_id}', '${token_in}', '${token_out}')`
      )}`;

      query += `ON CONFLICT DO NOTHING;`;

      // console.log(query);

      const res = await pool.query(query);
      console.log(res.rowCount, ' number of rows inserted');
    }
  }

  // shard1Logger.info(` ] }`);

  console.log('---------------------', '\n');
  return;
}

// To fetch token lists
let provider: InMemoryProvider | null;
let tokenMap: Map<string, TokenInfo>;
let tonicMarketMap: Map<string, any>;
let spinMarketMap: Map<number, any>;
let tokenSet = new Set<string>();
const notInTokenSet = new Set<string>();

(async () => {
  /**
   * Setup - fetch tonic markets
   */

  console.log('Setting up');
  const tokenList = localData.tokensMap as Array<TokenInfo>;
  // const tokens = await new TokenListProvider().resolve();
  // const tokenList = tokens.filterByNearEnv('mainnet').getList();
  // // console.log(tokenList);
  tokenMap = tokenList.reduce((map, item) => {
    map.set(item.address, item);
    tokenSet.add(item.address);
    return map;
  }, new Map<string, TokenInfo>());

  // Local data
  const tonicMarkets = localData.tonicMarkets;
  const spinMarkets = localData.spinMarkets as Array<SpinMarket>;
  const refPools = localData.refPools as Array<FormattedPool>;
  const refStablePools = localData.refStablePools;
  const jumboPools = localData.jumboPools;
  const jumboStablePools = localData.jumboStablePools;

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

  // Local data setup ends

  // provider = new InMemoryProvider(MainnetRpc, tokenMap);

  // await provider.fetchPools();

  // const tonicMarkets = provider.getTonicMarkets();
  // const spinMarkets = provider.getSpinMarkets();

  tonicMarketMap = tonicMarkets.reduce((map, item) => {
    map.set(item.id, item);
    return map;
  }, new Map<string, any>());

  spinMarketMap = spinMarkets.reduce((map, item) => {
    map.set(item.id, item);
    return map;
  }, new Map<number, any>());

  // fetch and store prices
  await fetchTokenPrices();

  await startStream(lakeConfig, handleStreamerMessage);
})();

// TODO: Before exiting make sure to write resultsRow to a file to pick up from where it stopped.
[
  `exit`,
  `SIGINT`,
  // `SIGUSR1`,
  // `SIGUSR2`,
  `uncaughtException`,
  `SIGTERM`,
].forEach((eventType) => {
  // process.on(eventType, cleanUpServer.bind(null, eventType));
});

function cleanUpServer(event: any) {
  // Can do only sync events here
  // TODO: In case of server stop, write data in results row to a txt file or something
  console.log('--------EXITING---------');
  console.log(event);
  console.log('Write result rows to a txt file');

  console.log(resultRows.size, ' number of txns are not written');

  // console.log(resultRows);

  for (const result of resultRows) {
    const receipt_id = result[0];
    const {
      blockHeight: block_height,
      blockTime: block_time,
      dex,
      sender,
      success,
      swaps,
    } = result[1];

    if (swaps.length == 0) {
      // Write to CSV
      txnCsvLogger.info('info', {
        receipt_id,
        block_height,
        block_time,
        pool_id: '',
        sender,
        amount_in: '',
        amount_out: '',
        token_in: '',
        token_out: '',
        success,
        dex,
      });
      continue;
    }

    for (const swap of swaps) {
      const { pool_id, token_in, amount_in, token_out, amount_out } = swap;

      // Write to CSV
      txnCsvLogger.info('info', {
        receipt_id,
        block_height,
        block_time,
        pool_id,
        sender,
        amount_in,
        amount_out,
        token_in,
        token_out,
        success,
        dex,
      });
    }
  }

  process.exit(1);
}

function takeActionsAndReturnArgs(actions: any): any {
  let result: JSON | null = null;

  // Handle FunctionCall
  if (actions['FunctionCall'] && actions['FunctionCall'].args) {
    const args = actions['FunctionCall'].args;

    try {
      const decodedArgs = Buffer.from(args.toString(), 'base64');
      const parsedJSONArgs = JSON.parse(decodedArgs.toString());

      result = parsedJSONArgs;
      //   console.log(`Decoded args ${JSON.stringify(parsedJSONArgs, null, 2)}`);
    } catch {
      // TODO: handle better, exit to investigate what went wrong maybe?
      result = null;
      console.log('FAILED INSIDE');
    }
  }
  return result;
}

// Fetch token prices every 30 mins
let tokenPrices = new Map<string, number>();
let cachedTokenPrices = new Map<string, number>();

setInterval(async () => {
  await fetchTokenPrices();
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
