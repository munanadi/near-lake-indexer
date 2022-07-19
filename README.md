#### Indexer

A simple indexer built on top of the [NEAR Lake Framework JS](https://github.com/near/near-lake-framework-js).

- [x] Able to read the receipts and filter out using `memo`
- [ ] Need to figure out how to check if transactions are successful or failed? `ft_resolve_transfer` ?
- [ ] How many tokens were actually exchanged?

> Need to have `~/.aws/credentials` with your AWS account to work

#### NOTES

- `shard.chunk.receipts' or `shard.chunk.transactions`cannot be used to parse for`memo` field
