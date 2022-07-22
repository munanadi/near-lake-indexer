import { startStream, types } from 'near-lake-framework';
import { createLogger, transports, format } from 'winston';
import CSV from 'winston-csv-format';
import { Big } from 'big.js';
import { TokenInfo, TokenListProvider } from '@tonic-foundation/token-list';
import { MainnetRpc } from 'near-workspaces';
import { InMemoryProvider, TonicMarket } from '@arbitoor/arbitoor-core';
import { Market as SpinMarket, Spin } from '@spinfi/core';

const txnCsvLogger = createLogger({
  level: 'info',
  format: CSV(
    [
      'receipt_id',
      'blocktime',
      'pool_id',
      'sender',
      'amount0',
      'amount1',
      'token0',
      'token1',
      'success',
      'dex',
    ],
    { delimiter: ',' }
  ),
  transports: [
    new transports.File({
      filename: './logs/good_txns.txt',
      level: 'info',
      maxFiles: 4000000,
    }),
  ],
});

const shard1Logger = createLogger({
  level: 'info',
  format: format.json(),
  transports: [
    new transports.File({
      filename: './logs/shard1.json',
      level: 'info',
      maxFiles: 4000000,
      format: format.combine(
        format.printf((info) => JSON.stringify(info.message))
      ),
    }),
  ],
});

const lakeConfig: types.LakeConfig = {
  s3BucketName: 'near-lake-data-mainnet',
  s3RegionName: 'eu-central-1',
  // startBlockHeight: 69253110, // Jumbo
  // startBlockHeight: 70223685, // Tonic
  // startBlockHeight: 69229361, // Ref
  startBlockHeight: 69328535, // Spin
};

const receiptsSetToTrack = new Set<string>();

const masterReceiptMap = new Map<string, string>();

const didTxnWork = new Map<string, boolean>();

const resultRows: {
  [receipt_id: string]: {
    blocktime: number;
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
  };
} = {};

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

  // shard1Logger.info(`{ \"block\": ${block.header.height} , \"chunks\": [ `);
  for (const shard of shards) {
    // console.log(JSON.stringify(shard, null, 2));
    // shard1Logger.info(JSON.stringify(shard));
    // shard1Logger.info(',');

    if (shard.receiptExecutionOutcomes) {
      const receiptExecutionOutcomes = shard.receiptExecutionOutcomes;

      for (const receiptExecutionOutcome of receiptExecutionOutcomes) {
        const { executionOutcome, receipt: ExeReceipt } =
          receiptExecutionOutcome;

        const {
          id,
          outcome: { logs, receiptIds, status, executorId },
        } = executionOutcome;

        const { receipt, receiptId, predecessorId } = ExeReceipt;

        if (receiptsSetToTrack.has(receiptId)) {
          console.log(`Receipt details of ${receiptId}}`);

          // console.log({
          //   id,
          //   receiptIds,
          //   status,
          //   executorId,
          //   receipt,
          //   receiptId,
          // });

          if (receipt['Action'] && receipt['Action'].actions) {
            for (const action of receipt['Action'].actions) {
              if (!action['FunctionCall']) {
                continue;
              }

              const methodName = action['FunctionCall'].methodName;

              const originalReceipt = masterReceiptMap.get(receiptId);

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
                      resultRows[originalReceipt]['swaps'].push({
                        pool_id: action.pool_id,
                        amount_in: action.amount_in,
                        amount_out: '0',
                        token_in: action.token_in,
                        token_out: action.token_out ?? '0',
                      });
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
                        const token_out = logSplit[5];

                        // console.log(log);

                        // Add to result row
                        resultRows[originalReceipt]['swaps'][i] = {
                          amount_in,
                          amount_out,
                          token_in,
                          token_out,
                          pool_id:
                            resultRows[originalReceipt]['swaps'][i].pool_id,
                        };

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
                          tonicMarketMap[pool_id].base_token['type'] === 'ft'
                            ? tonicMarketMap[pool_id].base_token.token_type[
                                'account_id'
                              ]
                            : tonicMarketMap[pool_id].base_token.token_type
                                .type;
                        quote_token =
                          tonicMarketMap[pool_id].quote_token['type'] === 'ft'
                            ? tonicMarketMap[pool_id].quote_token.token_type[
                                'account_id'
                              ]
                            : tonicMarketMap[pool_id].quote_token.token_type
                                .type;

                        break;
                      }
                      case 'spot.spin-fi.near': {
                        const params = JSON.parse(returnedJson['msg']);
                        pool_id = params['market_id'];

                        base_token = spinMarketMap[pool_id].base.address;
                        quote_token = spinMarketMap[pool_id].quote.address;
                        break;
                      }
                      default: {
                        console.log('-!!!!!!!!!!');
                      }
                    }

                    // console.log(JSON.stringify(tonicMarketMap[pool_id], null, 2));

                    resultRows[originalReceipt]['swaps'].push({
                      pool_id,
                      amount_in,
                      amount_out: '0',
                      token_in: base_token,
                      token_out: quote_token,
                    });

                    // Add the first receipt to track ft_trasnfer for output
                    console.log('receipts', receiptIds);
                    console.log('original', originalReceipt);

                    receiptIds.forEach((r) => {
                      receiptsSetToTrack.add(r); // Not sure if always the second one
                      masterReceiptMap.set(r, originalReceipt);
                    });
                  }
                }

                // console.log('POOL ACTIONS :', poolActions);
                // console.log('LOGS :', logs);

                // console.log(resultRows);
              } else if (methodName === 'ft_transfer') {
                // THIS WONT WORK AS EXECUTOR ID IS LOST
                // NEED TO DO SOMETHING TO RELEATE THIS TO TONIC.

                if (
                  predecessorId === 'v1.orderbook.near' ||
                  predecessorId === 'spot.spin-fi.near'
                ) {
                  // console.log(returnedJson, returnedJson['amount']);
                  const amount_out = returnedJson['amount'] ?? '0';

                  // const originalReceipt = masterReceiptMap.get(receiptId);

                  const result = resultRows[originalReceipt]['swaps'][0];
                  resultRows[originalReceipt]['swaps'][0] = {
                    ...result,
                    amount_out,
                  };

                  masterReceiptMap.delete(receiptId);
                }
              } else if (methodName === 'ft_resolve_transfer') {
                // Success txn check

                const success = status['SuccessValue'] ? true : false;

                if (success) {
                  const originalReceipt = masterReceiptMap.get(id);

                  didTxnWork.set(originalReceipt, true);

                  resultRows[originalReceipt]['success'] = true;

                  // Write to CSV
                  // txnCsvLogger.info('info', {
                  //   txn_hash: txnHash,
                  //   txn_blocktime: txnBlockTime,
                  //   pool_addr: data.poolState.toString(),
                  //   sender: data.sender.toString(),
                  //   amount0: data.amount0.toString(),
                  //   amount1: data.amount1.toString(),
                  // });
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
              masterReceiptMap.get(id)
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
                      // console.log(parsedJSONArgs);
                      // console.log({
                      //   id,
                      //   receiptIds,
                      //   status,
                      //   executorId,
                      //   receipt,
                      //   receiptId,
                      // });

                      // Add receipt ids to track
                      receiptIds.forEach((receipt) => {
                        masterReceiptMap.set(receipt, id);
                        receiptsSetToTrack.add(receipt);
                      });

                      // For succes tracking
                      didTxnWork.set(id, false);

                      resultRows[id] = {
                        dex,
                        blocktime: new Big(block.header.timestampNanosec)
                          .mul(new Big(10).pow(-9))
                          .mul(new Big(1000))
                          .toNumber(),
                        blockHeight: block.header.height,
                        sender: action.signerId ?? predecessorId ?? '',
                        success: false,
                        swaps: [],
                      };
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

  if (receiptsSetToTrack.size) {
    console.log(
      `Receipt Ids to track are ${Array.from(receiptsSetToTrack).map((r) =>
        r.toString()
      )}`
    );
  }

  console.log(JSON.stringify(resultRows, null, 2));

  // if (masterReceiptMap.size) {
  //   console.log('Master Receipt Table');
  //   console.table(masterReceiptMap);
  // }

  // if (didTxnWork.size) {
  //   console.log('Txn Success Table');
  //   console.table(didTxnWork);
  // }

  // shard1Logger.info(` ] }`);

  console.log('---------------------', '\n');
  return;
}

// To fetch token lists
let provider: InMemoryProvider | null;
let tokenMap: Map<string, TokenInfo>;
let tonicMarketMap: Map<string, TonicMarket>;
let spinMarketMap: Map<string, SpinMarket>;

(async () => {
  /**
   * Setup - fetch tonic markets
   */

  console.log('Setting up');
  const tokens = await new TokenListProvider().resolve();
  const tokenList = tokens.filterByNearEnv('mainnet').getList();
  // console.log(tokenList);
  tokenMap = tokenList.reduce((map, item) => {
    map[item.address] = item;
    return map;
  }, new Map<string, TokenInfo>());

  provider = new InMemoryProvider(MainnetRpc, tokenMap);

  await provider.fetchPools();

  const tonicMarkets = provider.getTonicMarkets();
  const spinMarkets = provider.getSpinMarkets();

  tonicMarketMap = tonicMarkets.reduce((map, item) => {
    map[item.id] = item;
    return map;
  }, new Map<string, TonicMarket>());

  spinMarketMap = spinMarkets.reduce((map, item) => {
    map[item.id] = item;
    return map;
  }, new Map<string, SpinMarket>());

  // console.log(tonicMarketMap);

  // for(const marketId of Object.keys(tonicMarketMap)){
  //   const {  } = tonicMarketMap.get(marketId]
  // }
  //   const { base_token, quote_token } =
  //   tonicMarketMap[action.market_id];

  // for (const m of Object.values(tonicMarketMap)) {
  //   console.log(m.id, m.base_token, m.quote_token);
  // }
  // console.log(Object.keys(tokenMap));

  await startStream(lakeConfig, handleStreamerMessage);
})();

function takeActionsAndReturnArgs(actions: types.ReceiptEnum): JSON {
  let result: JSON;
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
      console.log('FAILED INSIDE');
    }
  }
  return result;
}
