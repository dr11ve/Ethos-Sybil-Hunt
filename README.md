This script is a Telegram bot for multi-chain EVM wallet linkage analysis. You send it a wallet address, and it scans activity on Ethereum, Arbitrum, Base, Optimism, Polygon, and BSC to find wallets that may be controlled by the same entity.
What it can do:
* find direct wallet-to-wallet transfers with the target
* find wallets that use the same contract/deposit addresses
* detect repeated and bidirectional transfer patterns
* compare shared funding sources
* compare timing of outgoing transactions
* compare similar transfer amounts
* score candidate wallets and split them into:
    * strong linked cluster
    * possible candidates
* check the target and strong linked wallets against Ethos Network
* send the whole report back in Telegram
* resume interrupted scans using checkpoint files

To start you need to input the following DATA:
1. Alchemy API for Ethereum, Arbitrum, Base, Optimism and Polygon POS network (row 19)
   - Get API here > https://dashboard.alchemy.com/apps
2. ANKR API for BSC network (row 20)
   - Get API here > https://www.ankr.com/rpc/advanced-api
3. Create your own TG Bot using BOT father and Copy paste API
   - Search in TG > @BotFather
   - HTTP API copy paste into row 22
4. You need proxy url to make it smooth (row 414)
   - proxy_url = "http://Login:PASSWORD@IP:PORT"
   - ProxyLine Proxies > https://proxyline.net?ref=170348
