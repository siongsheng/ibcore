# Reddit Post Draft — r/interactivebrokers

**Title:** 6 months of IBKR API pain. I open-sourced the wrapper so you don't start from zero.

**Body:**

You know that moment when you're staring at an error code at 2am and the docs just say "General error occurred"?

I've been there. A lot. Spent 6 months building an automated options trading system on IBKR. Here's what nobody warns you about:

- Paper account returns stub data. The API doesn't tell you. Your code runs fine, your logic is correct, but the numbers are fake. Took me 3 days of doubting my own sanity before I figured it out.
- contract_details fails silently on far-OTM strikes. Works perfectly on SPY ATM. Then you scan the wings and everything breaks. No error. No hint. Just nothing.
- Gateway disconnects at 3am. You wake up, check your positions, and the Gateway's been down for 4 hours. No reconnect. No alert. Your P&L is whatever the market decided while you were sleeping.
- Commodity ETF prices jump from real to zero and back. GLD $186.25, then $0.00, then $186.25 again. Try building a screener on top of that.

The worst part? You go through the exact same debugging hell as everyone before you. Find a 4-year-old forum post. Try the workaround. Doesn't work. Try another. Sort of works. Move on. Nobody documents this stuff properly.

After 6 months I realized I'd accumulated a graveyard of undocumented IBKR quirks that nobody should have to rediscover from scratch. So I extracted the API layer:

**ibcore** — Rust + Python, 128 tests, MIT license.

Built on top of [ibapi](https://crates.io/crates/ibapi) (the excellent Rust IBKR client library). ibcore wraps it with typed errors, diagnostics, and the safety checks you'd end up writing yourself anyway.

https://crates.io/crates/ibcore | pip install ibcore-py

https://github.com/siongsheng/ibcore

Is it a finished trading platform? No. Will it auto-reconnect while you sleep? Not yet. But here's what it does that would have saved me months:

You know Error 10197? The one where your paper account and live account fight over market data and you get back all zeros? ibcore catches that. Retries with backoff. Won't silently feed your screener garbage bid/ask spreads.

You know Error 2107? "HMDS data farm connection is broken"? ibcore calls it `IbError::FarmDisconnect` and broadcasts it on a real-time diagnostics stream. You still have to write the reconnect loop yourself. But at least you'll know the Gateway died — instead of finding out when you wake up.

Wing strike contract resolution? Returns `IbError::ContractResolution` with the symbol, expiry, and strike right in the message. Still fails on some far-OTM contracts (it's not magic), but you'll know which one and why.

The bar here is low. The raw IB API gives you numeric codes and inconsistent docs. ib_insync was the gold standard (3,300 stars) but the author passed away in 2024 and the repo is archived. If you're starting an IBKR project today, you're either raw-dogging the API or using a dead library.

```python
from ibcore import IbClient

ib = IbClient.connect("127.0.0.1", 4002, 1, "delayed", "paper")
snap = ib.stock_snapshot("SPY")
print(f"SPY: ${snap.last:.2f}")
```

Still early. If you've hit IBKR quirks I haven't covered — or if this breaks for you — file an issue. Chances are someone else has the same problem and just hasn't found your forum post yet.
