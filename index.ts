import { startStream, types } from 'near-lake-framework';
import { createLogger, transports, format } from 'winston';
import CSV from 'winston-csv-format';
import { Big } from 'big.js';

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
  // startBlockHeight: 70223685,
  startBlockHeight: 69229361, // Ref
};

const receiptsSetToTrack = new Set<string>();

const masterReceiptMap = new Map<string, string>();

const didTxnWork = new Map<string, boolean>();

const resultRows: {
  [receipt_id: string]: {
    blocktime: string;
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

              if (
                ['callback_ft_on_transfer', 'ft_on_transfer'].includes(
                  methodName
                )
              ) {
                const returnedJson = takeActionsAndReturnArgs(action);

                const poolActions: any[] = [];

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
                      poolActions.push({
                        pool_id: action.pool_id,
                        token_in: action.token_in,
                        token_out: action.token_out,
                        amount_in: action.amount_in,
                        min_amount_out: action.min_amount_out ?? 0,
                      });
                    }
                  }
                } else if (['v1.orderbook.near'].includes(executorId)) {
                  const actionType = returnedJson['msg']['action'];
                  const params = returnedJson['msg']['params'];

                  for (const action of params) {
                    if (actionType == 'Swap') {
                      // TODO: Need to figure out data structure for this later
                      poolActions.push({
                        pool_id: action.market_id,
                      });
                    } else {
                      console.log(`TYPE IS ${actionType}`);
                    }
                  }
                }

                const originalReceipt = masterReceiptMap.get(receiptId);

                if (poolActions.length) {
                  for (const action of poolActions) {
                    // Fill the pool_id, token_in, token_out, amount_in
                    // console.log(originalReceipt, ' original Receipt');
                    // console.log(resultRows);

                    resultRows[originalReceipt]['swaps'].push({
                      pool_id: action.pool_id,
                      amount_in: action.amount_in,
                      amount_out: '0',
                      token_in: action.token_in,
                      token_out: action.token_out,
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

                // console.log('POOL ACTIONS :', poolActions);
                // console.log('LOGS :', logs);

                // console.log(resultRows);
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
                        blocktime: block.header.timestampNanosec,
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

  console.log(
    Object.keys(resultRows).map((row) => {
      const { blocktime, dex, sender, success, swaps } = resultRows[row];
      return `${row} -> ${new Date(
        new Big(blocktime)
          .mul(new Big(10).pow(-9))
          .mul(new Big(1000))
          .toNumber()
      ).toString()}  ${dex} ${sender} ${
        success ? 'SUCCESS' : 'FAIL'
      } \nSWAPS : \n${swaps.map(
        ({ amount_in, amount_out, pool_id, token_in, token_out }) =>
          `${pool_id} : ${token_in} ${amount_in} => ${token_out} ${amount_out} ${'\n'}`
      )}`;
    })
  );

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

(async () => {
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
