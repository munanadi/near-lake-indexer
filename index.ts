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

  console.log(`Block #${block.header.height} Shards: ${shards.length}`);

  //   shard1Logger.info(`{ "block": ${block.header.height} , "chunks": [ `);
  for (const shard of shards) {
    // console.log(JSON.stringify(shard, null, 2));
    // shard1Logger.info(shard);
    // shard1Logger.info(',');

    if (shard.receiptExecutionOutcomes) {
      for (const receipt of shard.receiptExecutionOutcomes) {
        if (receiptSet.has(receipt.receipt.receiptId)) {
          console.log(
            `Receipt of ${receipt.receipt.receiptId} is \n ${JSON.stringify(
              receipt.receipt,
              null,
              2
            )}`
          );

          if (
            receipt.receipt.receipt['Action'] &&
            receipt.receipt.receipt['Action'].actions
          ) {
            for (const action of receipt.receipt.receipt['Action'].actions) {
              console.log(action);
              const returnedJson = takeActionsAndReturnArgs(action);
              console.log('Does this work? ', returnedJson);
            }
          }

          // Delete receipt id after tracking
          receiptSet.delete(receipt.receipt.receiptId);
        } else if (
          receipt.receipt.receipt['Action'] &&
          receipt.receipt.receipt['Action'].actions
        ) {
          for (const action of receipt.receipt.receipt['Action'].actions) {
            // console.log(action);
            if (
              action['FunctionCall'] &&
              action['FunctionCall'].methodName === 'ft_transfer_call'
            ) {
              const args = action['FunctionCall'].args;

              try {
                const decodedArgs = Buffer.from(args.toString(), 'base64');
                const parsedJSONArgs = JSON.parse(decodedArgs.toString());
                const memo = parsedJSONArgs['memo'] ?? '';

                if (memo === 'arbitoor') {
                  console.log(parsedJSONArgs);
                  console.log(receipt.executionOutcome);

                  // Add receipt ids to track
                  receipt.executionOutcome.outcome.receiptIds.forEach(
                    (receipt) => receiptSet.add(receipt)
                  );

                  //  // Add Status receipt ids to track
                  //  if (
                  //     receipt.executionOutcome.outcome.status['SuccessReceiptId']
                  //   ) {
                  //     receiptSet.add(
                  //       receipt.executionOutcome.outcome.status[
                  //         'SuccessReceiptId'
                  //       ]
                  //     );
                  //   }
                }
              } catch {
                // TODO: handle better, exit to investigate what went wrong maybe?
                console.log('FAILED');
              }
            }
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

  //   shard1Logger.info(` ] }`);

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
