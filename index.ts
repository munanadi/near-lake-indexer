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
import localData from './scripts/data/data.json';

const startingBlock = +process.argv.slice(2)[0];

// To keep track of toekns that are not in the tonic list
const txnErrLogger = createLogger({
  level: 'info',
  format: CSV(['receipt_id', 'block_height'], {
    delimiter: ',',
  }),
  transports: [
    new transports.File({
      filename: './logs/failed-txns.txt',
      level: 'info',
    }),
  ],
});

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
    swapSuccess: boolean;
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

        const { receiptId, predecessorId, receiverId } = ExeReceipt;
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
                        amount_in: action.amount_in ?? '0',
                        amount_out: '0',
                        token_in: action.token_in,
                        token_out: action.token_out ?? '',
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
                    // Check for errors from the smart contract. In result of this part
                    if (status && status['Failure']) {
                      console.log('###########################');
                      console.log(
                        'SKIPPING THIS AS FAILURE',
                        JSON.stringify(status['Failure'])
                      );

                      // Remove sub receipts that are tracked for this original receipt
                      for (const receipt of Array.from(receiptsSetToTrack)) {
                        if (masterReceiptMap.get(receipt) === originalReceipt) {
                          receiptsSetToTrack.delete(receipt);
                          masterReceiptMap.delete(receipt);
                        }
                      }

                      // Write to table with swapSuccess as false
                      const data = resultRows.get(originalReceipt)!;

                      resultRows.set(originalReceipt, {
                        ...data,
                        success: true,
                      });

                      continue;
                    }

                    const amount_in = returnedJson['amount'];

                    const swapTemp: {
                      pool_id: string;
                      base_token: string;
                      quote_token: string;
                      min_output_amount: string;
                    }[] = [];

                    if (executorId === 'spot.spin-fi.near') {
                      const params = JSON.parse(returnedJson['msg']);
                      const pool_id = params['market_id'];

                      const base_token =
                        spinMarketMap.get(+pool_id)?.base.address ?? '';
                      const quote_token =
                        spinMarketMap.get(+pool_id)?.quote.address ?? '';

                      swapTemp.push({
                        pool_id,
                        base_token,
                        quote_token,
                        min_output_amount: '0',
                      });

                      const newSwapsArr =
                        resultRows.get(originalReceipt)?.swaps ?? [];
                      const oldData = resultRows.get(originalReceipt)!;

                      // NOTE: Doesn't really make sense to array this now, But lets refactor this when we parse spin logs
                      for (const swap of swapTemp) {
                        const {
                          pool_id,
                          base_token,
                          quote_token,
                          min_output_amount,
                        } = swap;

                        newSwapsArr.push({
                          pool_id,
                          amount_in,
                          amount_out: min_output_amount ?? '0',
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
                      }

                      // NOTE: Need to track further receipts only for spot.
                      // As we parse logs and figure out tonic
                      receiptIds.forEach((r) => {
                        receiptsSetToTrack.add(r);
                        masterReceiptMap.set(r, originalReceipt);
                      });
                    } else if (executorId === 'v1.orderbook.near') {
                      const params = JSON.parse(returnedJson['msg'])['params'];

                      const swapsData = new Map<
                        string,
                        {
                          pool_id: string;
                          side: string;
                          amount_in: string;
                          amount_out: string;
                          token_in: string;
                          token_out: string;
                        }
                      >();

                      // Parsing logs to get 'Fill' and 'Order'
                      for (const log of logs) {
                        const jsonLog = JSON.parse(log);

                        const { type, data } = jsonLog;

                        switch (type) {
                          case 'Fill': {
                            const pool_id = data['market_id'].toString();
                            const order_id = data['order_id'].toString();

                            const token_in =
                              (tonicMarketMap.get(pool_id)?.base_token as any)
                                .token_type['type'] === 'ft'
                                ? (
                                    tonicMarketMap.get(pool_id)
                                      ?.base_token as any
                                  ).token_type['account_id']
                                : (
                                    tonicMarketMap.get(pool_id)
                                      ?.base_token as any
                                  ).type;
                            const token_out =
                              (tonicMarketMap.get(pool_id)?.quote_token as any)
                                .token_type['type'] === 'ft'
                                ? (
                                    tonicMarketMap.get(pool_id)
                                      ?.quote_token as any
                                  ).token_type['account_id']
                                : (
                                    tonicMarketMap.get(pool_id)
                                      ?.quote_token as any
                                  ).type;

                            // Will add to set to fetch meta data
                            if (!tokenSet.has(token_in)) {
                              notInTokenSet.add(token_in);
                            }
                            if (!tokenSet.has(token_out)) {
                              notInTokenSet.add(token_out);
                            }

                            let fillQty = BigInt('0');
                            let quoteQty = BigInt('0');

                            for (const fill of data['fills']) {
                              fillQty += BigInt(fill['fill_qty']);
                              quoteQty += BigInt(fill['quote_qty']);
                            }

                            const order = {
                              pool_id,
                              token_in: token_in.toString(),
                              token_out: token_out.toString(),
                              side: 'XXX',
                              amount_in: fillQty.toString(),
                              amount_out: quoteQty.toString(),
                            };

                            swapsData.set(order_id, order);

                            break;
                          }
                          case 'Order': {
                            const order_id = data['order_id'].toString();
                            const side = data['side'].toString();

                            const oldSwapData = swapsData.get(order_id);

                            if (!oldSwapData) {
                              console.log(
                                'swap data not found? Out of order? Skipping',
                                order_id
                              );
                            } else {
                              // update the side for this order

                              // if buy flip order of tokens and set input to fillQty
                              // else input to quoteQty
                              if (side === 'Buy') {
                                swapsData.set(order_id, {
                                  ...oldSwapData,
                                  token_in: oldSwapData.token_out,
                                  token_out: oldSwapData.token_in,
                                  amount_in: oldSwapData.amount_out.toString(),
                                  amount_out: oldSwapData.amount_in.toString(),
                                  side,
                                });
                              } else {
                                swapsData.set(order_id, {
                                  ...oldSwapData,
                                  side,
                                });
                              }
                            }

                            break;
                          }
                          default: {
                            console.log('UNKNOWN TYPE', type);
                          }
                        }
                      }

                      // console.log('LOGS', logs);
                      for (const swapData of swapsData) {
                        const {
                          amount_in,
                          token_in,
                          amount_out,
                          token_out,
                          pool_id,
                        } = swapData[1];

                        const token0Decimal =
                          tokenMap.get(token_in)?.decimals ?? 1;
                        const token1Decimal =
                          tokenMap.get(token_out)?.decimals ?? 1;

                        console.log('\t', swapData[0], ' order ID');
                        console.log(
                          '\t',
                          new Big(amount_in)
                            .mul(new Big('10').pow(-token0Decimal))
                            .toString(),
                          token_in,
                          ' => ',
                          new Big(amount_out)
                            .mul(new Big('10').pow(-token1Decimal))
                            .toString(),
                          token_out
                        );

                        const newSwapsArr =
                          resultRows.get(originalReceipt)?.swaps ?? [];
                        const oldData = resultRows.get(originalReceipt)!;

                        newSwapsArr.push({
                          amount_in,
                          amount_out,
                          pool_id,
                          token_in,
                          token_out,
                        });

                        resultRows.set(originalReceipt, {
                          ...oldData,
                          success: true,
                          swapSuccess: true,
                          swaps: newSwapsArr,
                        });

                        // masterReceiptMap.delete(receiptId);
                      }

                      // Remove receitps ids related to original recepipt as no tracking required any longer
                      for (const receipt of Array.from(receiptsSetToTrack)) {
                        if (
                          originalReceipt ===
                            masterReceiptMap.get(receipt.toString()) ??
                          ''
                        ) {
                          receiptsSetToTrack.delete(receipt);
                        }
                      }
                    } else {
                      console.log('!!!!!!!!!!!!!!!!!!!!!!1');
                      break;
                    }
                  }
                }
              } else if (
                methodName === 'ft_transfer' &&
                predecessorId === 'spot.spin-fi.near'
              ) {
                const finalAmountOut = returnedJson['amount'] ?? '0';

                // Handle for multiple swaps
                for (const swap of resultRows.get(originalReceipt)?.swaps ??
                  []) {
                  if (!swap) continue;

                  let { amount_in, amount_out, pool_id, token_in, token_out } =
                    swap;

                  const oldData = resultRows.get(originalReceipt)!;

                  // For spin check if the pools are inverted
                  const quote_token = receiverId;
                  if (quote_token === token_in) {
                    let tmp = token_out;
                    token_out = token_in;
                    token_in = tmp;
                  }

                  resultRows.set(originalReceipt, {
                    ...oldData,
                    swaps: [
                      {
                        amount_in,
                        amount_out: finalAmountOut,
                        token_in,
                        token_out,
                        pool_id,
                      },
                    ],
                  });
                }

                masterReceiptMap.delete(receiptId);
              } else if (methodName === 'ft_resolve_transfer') {
                // Success txn check

                const success = status['SuccessValue'] ? true : false;

                if (success) {
                  didTxnWork[originalReceipt] = true;

                  const oldData = resultRows.get(originalReceipt)!;

                  resultRows.set(originalReceipt, {
                    ...oldData,
                    success: true,
                    swapSuccess: true,
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
                        swapSuccess: false,
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
      console.log(
        `\t ${pool_id} ${
          token0Decimals >= 15
            ? new Big(
                new Big(amount_in).mul(new Big(10).pow(-15)).toString()
              ).mul(new Big(10).pow(-(token0Decimals - 15)))
            : new Big(amount_in)
                .mul(new Big(10).pow(-token0Decimals))
                .toString()
        } ${token_in} -> ${
          token1Decimals >= 15
            ? new Big(
                new Big(amount_out).mul(new Big(10).pow(-15)).toString()
              ).mul(new Big(10).pow(-(token1Decimals - 15)))
            : new Big(amount_out)
                .mul(new Big(10).pow(-token1Decimals))
                .toString()
        } ${token_out}`
      );
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
      const {
        blockHeight,
        blockTime,
        dex,
        sender,
        success,
        swapSuccess,
        swaps,
      } = result[1];

      // Add only succeded swaps
      if (!success) {
        continue;
      }

      // Entry for swap failures
      if (!swaps.length) {
        // Write to a logger locally
        txnErrLogger.info('info', {
          receipt_id: txn,
          block_height: blockHeight,
        });

        swapRows.push({
          txn,
          blockHeight,
          blockTime,
          dex,
          sender,
          success: swapSuccess,
          amount_in: '0',
          amount_in_d: '0',
          amount_in$: '0',
          amount_in$_d: '0',
          amount_out: '0',
          amount_out_d: '0',
          amount_out$: '0',
          amount_out$_d: '0',
          pool_id: '',
          token_in: '',
          token_out: '',
        });
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
          success: swapSuccess,
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
   * Setup - Fetch Markets and Token prices
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

  tonicMarketMap = tonicMarkets.reduce((map: any, item: any) => {
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
