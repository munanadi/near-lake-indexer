import { startStream, types } from 'near-lake-framework';
import { createLogger, transports, format } from 'winston';

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
  startBlockHeight: 69253110,
};

const receiptSet = new Set<string>();

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
          outcome: { logs, receiptIds, status },
        } = executionOutcome;

        const { receipt, receiptId } = ExeReceipt;

        if (receiptSet.has(receiptId)) {
          console.log(`Receipt of ${receiptId} is \n ${receipt}`);

          if (receipt['Action'] && receipt['Action'].actions) {
            for (const action of receipt['Action'].actions) {
              const returnedJson = takeActionsAndReturnArgs(action);
              console.log(Object.keys(action), returnedJson);
              console.log('LOGS: ', logs);
            }
          }

          // Delete receipt id after tracking
          receiptSet.delete(receiptId);
        }

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

                    if (memo === 'arbitoor') {
                      console.log(parsedJSONArgs);
                      console.log(id, status);

                      // Add receipt ids to track
                      receiptIds.forEach((receipt) => receiptSet.add(receipt));

                      // Add Status receipt ids to track
                      if (status['SuccessReceiptId']) {
                        receiptSet.add(status['SuccessReceiptId']);
                      }

                      if (status['SuccessValue']) {
                        receiptSet.add(status['SuccessValue']);
                      }
                    }
                  } catch {
                    // TODO: handle better, exit to investigate what went wrong maybe?
                    console.log('FAILED');
                  }
                }
                //  else {
                //   console.log('FN CALL', action['FunctionCall'].methodName);
                // }
              }
            }
            //  else if (action['Transfer']) {
            //   console.log('TRANSFER', action['Transfer']);
            // } else {
            //   console.log('OTHER', action);
            // }
          }
        }
      }
    }
  }

  if (receiptSet.size) {
    console.log(
      `Receipt Ids to track are ${Array.from(receiptSet).map((r) =>
        r.toString()
      )}`
    );
  }

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
