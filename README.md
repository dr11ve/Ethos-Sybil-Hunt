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
4. You need proxy url to make it smooth (row 420)
   - proxy_url = "http://Login:PASSWORD@IP:PORT"
   - ProxyLine Proxies > https://proxyline.net?ref=170348

To clarify, the final output does not always 100% guarantee that tightly linked addresses belong to a single owner. However, in approximately 85%/90% of cases, it strongly indicates that they're controlled by the same owner. If you receive multiple wallets in the final results, I recommend checking them through Nansen AI (if you have access) or ([Arkham Intelligence]([url](https://intel.arkm.com/visualizer)) to verify any connections between the wallets. This step is very helpful. In rare cases, addresses may have simply interacted with the same uncommon or low-popularity smart contracts, causing the script to flag them as linked wallets.
That said, the script’s overall confidence is still very high, typically between 85% and 90%.
Here are two examples below: 

A) one where the script correctly detected real farming accounts:

<img width="425" height="373" alt="Lensixm Sybil" src="https://github.com/user-attachments/assets/fcc5710a-92f2-411a-a32e-49353d286341" />

B) and another showing two separate wallets belonging to two different people:

<img width="420" height="216" alt="Need3Sleep Human" src="https://github.com/user-attachments/assets/2057cdbb-eee6-4f99-919d-c9a71a270a1c" />



