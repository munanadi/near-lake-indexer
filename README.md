#### Indexer

A simple indexer built on top of the [NEAR Lake Framework JS](https://github.com/near/near-lake-framework-js).

- [ ] Need to add a new daily volumes table that will be filled by the cronjob run every night.
- [ ] Need to handle failed transactions that are terminated in between due to slippage or other more efficiently.
      look for `amounts` with 0

- [x] Handled multiplication errors with dividing twice. (i.e `* 10 ** -24` => `(* 10 ** -15) * (10 ** -9)` now )
- [x] How to relate to the receipt ids that are parsed from a txns initially and map them back to this starting point.
- [x] What kind of table schema to store?
- [x] ~~How many tokens were actually exchanged? Spin has brosh seriazlized logs.~~ Did away with other methods
- [x] Able to read the receipts and filter out using `memo`
- [x] Need to figure out how to check if transactions are successful or failed? `ft_resolve_transfer` ?

> Need to have `~/.aws/credentials` with your AWS account to work

#### NOTES

- `shard.chunk.receipts' or `shard.chunk.transactions`cannot be used to parse for`memo` field
- Need to have `arbitoor_txns` table in local postgres before running the script. Schema [here](https://github.com/pisomanik/near-lake-indexer#table-schema)
- Create `logs/not_written.txt` to catch all un written txns into a csv file to continue later
- Takes a arg for starting block
- Required `near-cli` to be installed to fetch markets for tonic and spin

```
ts-node index.ts 19283018 // This will start indexing from the given block
```

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

`numeric` might be bad for `token_in` and `token_out` but will need to see.

```
 \d arbitoor_txns
                   Table "public.arbitoor_txns"
    Column     |       Type       | Collation | Nullable | Default 
---------------+------------------+-----------+----------+---------
 receipt_id    | text             |           | not null | 
 block_height  | bigint           |           |          | 
 blocktime     | bigint           |           |          | 
 dex           | text             |           |          | 
 sender        | text             |           |          | 
 success       | boolean          |           |          | 
 amount_in$    | numeric          |           |          | 
 amount_out$   | numeric          |           |          | 
 amount_in$_d  | double precision |           |          | 
 amount_out$_d | double precision |           |          | 
 amount_in     | numeric          |           |          | 
 amount_out    | numeric          |           |          | 
 amount_in_d   | double precision |           |          | 
 amount_out_d  | double precision |           |          | 
 pool_id       | text             |           | not null | 
 token_in      | text             |           |          | 
 token_out     | text             |           |          | 
Indexes:
    "arbitoor_txns_pkey" PRIMARY KEY, btree (receipt_id, pool_id)

```

#### Connect to public DB using psql

```
 psql -h mainnet.db.explorer.indexer.near.dev -U public_readonly -d mainnet_explorer
 // You will be prompted for password: nearprotocol
```

#### RESULTS

##### 1. Ref

```
{
  "7BGhx36rurv1str4zzzhzZVeV5dNFamBoSbcUhVfCnLB": {
    "dex": "v2.ref-finance.near",
    "blocktime": 1657085241626.2317,
    "blockHeight": 69229361,
    "sender": "spoiler.near",
    "success": true,
    "swaps": [
      {
        "amount_in": "744477563434064562397229",
        "amount_out": "676754020909614668",
        "token_in": "wrap.near",
        "token_out": "marmaj.tkn.near",
        "pool_id": 11
      },
      {
        "amount_in": "676754020909614668",
        "amount_out": "2525254141954403206",
        "token_in": "marmaj.tkn.near",
        "token_out": "usn",
        "pool_id": 3135
      },
      {
        "amount_in": "255522436565935437602771",
        "amount_out": "9482415898108003692",
        "token_in": "wrap.near",
        "token_out": "token.pembrock.near",
        "pool_id": 3449
      },
      {
        "amount_in": "9482415898108003692",
        "amount_out": "864815496287069361",
        "token_in": "token.pembrock.near",
        "token_out": "usn",
        "pool_id": 3448
      }
    ]
  }
}
```

##### 2. Jumbo

```
{
  "Cv4EFz3jzRETvBLvstvMDDRrdRQMmfCNs1JFEvsTc5SF": {
    "dex": "v1.jumbo_exchange.near",
    "blocktime": 1657115834059.8887,
    "blockHeight": 69253111,
    "sender": "spoiler.near",
    "success": false,
    "swaps": [
      {
        "amount_in": "2864400000000000000",
        "amount_out": "2863230",
        "token_in": "usn",
        "token_out": "dac17f958d2ee523a2206206994597c13d831ec7.factory.bridge.near,",
        "pool_id": 248
      },
      {
        "amount_in": "2863230",
        "amount_out": "832850811100407145229804",
        "token_in": "dac17f958d2ee523a2206206994597c13d831ec7.factory.bridge.near",
        "token_out": "wrap.near",
        "pool_id": 1
      }
    ]
  }
}

```

##### 3. Tonic

```
{
  "3zWnEX3rbY2DhqJjtFjSKXD3way1Wc2Wk3ks6aaBSXLV": {
    "dex": "v1.orderbook.near",
    "blocktime": 1658278151723.6606,
    "blockHeight": 70223685,
    "sender": "skiran017.near",
    "success": true,
    "swaps": [
      {
        "pool_id": "J5mggeEGCyXVUibvYTe9ydVBrELECRUu23VRk2TwC2is",
        "amount_in": "409597973",
        "amount_out": "409340000000000000000",
        "token_in": "usn",
        "token_out": "a0b86991c6218b36c1d19d4a2e9eb0ce3606eb48.factory.bridge.near"
      }
    ]
  }
}
```

##### 4. Spin

```
{
  "F7oRXo6caLhv8YfceTmKFU1DUykvVDjqQhbU7X3wB7oJ": {
    "dex": "spot.spin-fi.near",
    "blocktime": 1657213857934.6562,
    "blockHeight": 69328535,
    "sender": "skiran017.near",
    "success": true,
    "swaps": [
      {
        "pool_id": 2,
        "amount_in": "55661100000000000000",
        "amount_out": "55635890",
        "token_in": "usn",
        "token_out": "a0b86991c6218b36c1d19d4a2e9eb0ce3606eb48.factory.bridge.near"
      }
    ]
  }
}
```

##### ERRROS

1. Bad gateway error while fetching tonic markets

```
BadGatewayError:
<html><head>
<meta http-equiv="content-type" content="text/html;charset=utf-8">
<title>502 Server Error</title>
</head>
<body text=#000000 bgcolor=#ffffff>
<h1>Error: Server Error</h1>
```

2. S3 sometimes time out, retries automatically though
