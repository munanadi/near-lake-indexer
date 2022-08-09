### `cronjob.ts` [WIP]

This script is to populate daily volume of tokens flow.

---

### `populateData.ts`

This would pull markets among the existing dexes and create a token map of all the tokens.

This list then can be used in nodes where we would require decimal information of a token.

`tokens-data.json` would contain a array of `TokenInfo` entries.

> Uses the near enhanced api to fetch token meta data information
