#### Indexer

A simple indexer built on top of the [NEAR Lake Framework JS](https://github.com/near/near-lake-framework-js).

- [ ] How to relate to the receipt ids that are parsed from a txns initially and map them back to this starting point.
- [ ] What kind of table schema to store?
- [ ] ~~How many tokens were actually exchanged?~~ Spin has brosh seriazlized logs.
- [x] Able to read the receipts and filter out using `memo`
- [x] Need to figure out how to check if transactions are successful or failed? `ft_resolve_transfer` ?

> Need to have `~/.aws/credentials` with your AWS account to work

#### NOTES

- `shard.chunk.receipts' or `shard.chunk.transactions`cannot be used to parse for`memo` field


#### PLAN

There are four Dexes to track transactions for currently -Ref, Jumbo, Tonic, Spin- which can be split into two types - AMM's and Orderbooks

> Spin has borsh serialized logs so skipping it for now

1. All transactions have a `ft_transfer_call` which has actions that can be parsed for `memo` field which can be used to filter transactions
 - This further has receipts ids that you get for the further executions.

2. The receipt ids that are collected are tracked for further investigation. 
 - Need to figure out how to map what receipt Ids are related to which ones? 

3. `ft_on_transfer` has logs that can be used for Ref pools, Tonic and Spin markets
 - These can be fetched from `1.` receiptIds itself. And no further fetching of receipts Ids are required.

> Spin borsh serialized logs can be found here too.

4. `callback_ft_on_transfer` has logs that can be used for Jumbo pools
  - This might need to track on more `status` and look for that receipt id in the subsequent blocks too.

5. Lastly `ft_resolve_transfer` indicates that a swap actually went through.

##### Table Schema

| `pool_id` or `market_id` | `token_in` | `token_out` | `amount_in` | `amount_out` | `dex` | `txn_hash` or `receipt_id` |
| ------------------------ | ---------  | ----------  | ----------  | -----------  | ----  |  -----------------------   |