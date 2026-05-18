"""
generate_sample_data.py
-----------------------
Generate realistic sample JSON files for the PySpark pipeline.
Creates:
  - data/sample/news_articles.json  (50+ financial news articles)
  - data/sample/stock_prices.json   (7 days of price bars for 10 tickers)

Run:
    python data/sample/generate_sample_data.py
"""

import json
import os
import random
from datetime import datetime, timedelta, timezone

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

# ---------------------------------------------------------------------------
# News articles
# ---------------------------------------------------------------------------
SOURCES = [
    "Reuters Business", "Yahoo Finance", "CNBC Top News",
    "MarketWatch", "Seeking Alpha", "Investing.com",
]

TICKERS = ["AAPL", "TSLA", "MSFT", "GOOGL", "AMZN", "NVDA", "BTC-USD", "ETH-USD"]

HEADLINES = [
    ("AAPL",  "positive", "Apple reports record-breaking iPhone sales in Q2",
     "Apple Inc. posted quarterly revenue of $94.8 billion, beating analyst estimates by 3.2%, driven by strong iPhone 15 Pro demand in China and India."),
    ("AAPL",  "neutral",  "Apple announces new sustainability targets for 2030",
     "The Cupertino giant outlined plans to achieve carbon neutrality across its entire supply chain by 2030, though analysts see limited near-term financial impact."),
    ("AAPL",  "negative", "Apple faces antitrust probe in EU over App Store fees",
     "European Commission launched a formal investigation into Apple's 30% commission on App Store purchases, which could result in fines up to 10% of global revenue."),
    ("TSLA",  "negative", "Tesla recalls 300,000 vehicles over Autopilot concerns",
     "NHTSA ordered a recall affecting Model 3 and Model Y vehicles manufactured between 2021 and 2024 due to phantom braking issues in Autopilot mode."),
    ("TSLA",  "positive", "Tesla Cybertruck deliveries exceed expectations",
     "Tesla delivered 42,000 Cybertrucks in Q1, surpassing Wall Street's estimate of 28,000, with average selling price holding above $78,000."),
    ("TSLA",  "negative", "Tesla China sales drop 18% month-over-month",
     "Insurance registration data shows Tesla's China sales fell sharply as BYD and Xiaomi gained market share with aggressive pricing strategies."),
    ("MSFT",  "positive", "Microsoft Azure revenue grows 31% on AI demand",
     "Microsoft's cloud division reported accelerating growth as enterprise customers adopted Azure AI services, contributing $2.5 billion in incremental revenue."),
    ("MSFT",  "negative", "Microsoft Azure outage disrupts enterprise customers",
     "A multi-hour Azure cloud outage affected thousands of enterprise customers in Europe and the Middle East, raising reliability concerns."),
    ("MSFT",  "neutral",  "Microsoft expands Copilot AI to all Office 365 tiers",
     "Microsoft announced general availability of Copilot AI assistant across all Office 365 subscription tiers, though enterprise adoption rates remain uncertain."),
    ("GOOGL", "positive", "Google Cloud wins $1.2B federal contract",
     "Alphabet's Google Cloud division secured a major federal government contract for AI-powered data analytics, beating out AWS and Azure in the bidding process."),
    ("GOOGL", "negative", "Alphabet ad revenue growth slows amid AI search disruption",
     "Google's parent company beat earnings but guidance disappointed as analysts flagged structural risk from AI-powered search alternatives eroding ad volume."),
    ("GOOGL", "neutral",  "Google DeepMind publishes breakthrough protein folding research",
     "The research advances protein structure prediction accuracy by 15%, though commercial applications remain 2-3 years away according to analysts."),
    ("AMZN",  "positive", "Amazon AWS profit margin hits all-time high of 38%",
     "AWS operating margin expanded to 38.2%, driven by higher-margin AI inference workloads and improved data center efficiency."),
    ("AMZN",  "negative", "Amazon logistics costs weigh on Q1 operating margin",
     "Amazon's first-quarter results showed AWS revenue growth but higher-than-expected fulfilment costs compressed margins in North America retail."),
    ("AMZN",  "neutral",  "Amazon announces Prime Day dates for July",
     "Amazon confirmed its annual Prime Day sale event for July 16-17, with early analyst estimates projecting $14 billion in gross merchandise value."),
    ("NVDA",  "positive", "NVIDIA shares surge after record quarterly earnings",
     "NVIDIA Corporation reported quarterly revenue of $26 billion, crushing estimates of $24.5 billion, driven by insatiable demand for H100 and B200 AI chips."),
    ("NVDA",  "positive", "NVIDIA announces next-gen Blackwell Ultra architecture",
     "Jensen Huang unveiled the Blackwell Ultra GPU platform at GTC, promising 4x inference performance gains, sending shares up 8% in after-hours trading."),
    ("NVDA",  "negative", "NVIDIA faces export restriction risks to China",
     "New US export controls could limit NVIDIA's China revenue by up to $5 billion annually, with modified H20 chips also under review for restrictions."),
    ("BTC-USD", "positive", "Bitcoin breaks $63K resistance — analysts eye $70K",
     "Bitcoin cleared a key technical resistance level overnight, with on-chain data showing large wallet accumulation and a sharp drop in exchange outflows."),
    ("BTC-USD", "negative", "Bitcoin drops 5% after Mt. Gox repayment fears",
     "Cryptocurrency markets tumbled as the Mt. Gox trustee moved $2.7 billion in BTC to exchange wallets, raising fears of imminent sell pressure."),
    ("BTC-USD", "neutral",  "Bitcoin ETF inflows slow to $120M daily average",
     "Spot Bitcoin ETF net inflows decelerated from the $500M daily pace seen in March, though cumulative AUM continues to grow steadily."),
    ("ETH-USD", "positive", "Ethereum ETF net inflows hit monthly record",
     "Spot Ethereum ETFs recorded their highest single-month net inflow since approval, with BlackRock and Fidelity products leading the tally."),
    ("ETH-USD", "negative", "Ethereum gas fees spike amid memecoin frenzy",
     "Average Ethereum transaction fees surged to $45 as a wave of memecoin launches congested the network, frustrating DeFi users."),
    ("ETH-USD", "neutral",  "Ethereum Dencun upgrade reduces L2 fees by 90%",
     "The Dencun hard fork successfully activated, slashing Layer 2 rollup fees dramatically, though impact on ETH price remains muted."),
    ("AAPL",  "positive", "Apple Vision Pro enterprise adoption accelerates",
     "Fortune 500 companies are deploying Apple Vision Pro for industrial design and medical training, with enterprise orders up 200% quarter-over-quarter."),
    ("TSLA",  "positive", "Tesla Energy division revenue doubles year-over-year",
     "Tesla's Megapack battery deployments reached 10.4 GWh in Q1, with the energy division now generating higher margins than the automotive segment."),
    ("MSFT",  "positive", "Microsoft Teams surpasses 400 million monthly active users",
     "Microsoft's collaboration platform hit a new milestone, driven by AI-powered meeting summaries and real-time translation features."),
    ("GOOGL", "positive", "YouTube ad revenue grows 21% to $8.9 billion",
     "YouTube's advertising business accelerated as Shorts monetization improved and connected TV viewership expanded by 30% year-over-year."),
    ("AMZN",  "positive", "Amazon announces $150B data center investment plan",
     "Amazon committed to investing $150 billion over 15 years in global data center infrastructure to meet surging AI and cloud computing demand."),
    ("NVDA",  "neutral",  "NVIDIA stock added to Dow Jones Industrial Average",
     "NVIDIA replaced Intel in the Dow Jones Industrial Average, reflecting the semiconductor industry's shift toward AI-focused chipmakers."),
    ("AAPL",  "negative", "Apple iPhone sales decline 10% in China market",
     "Apple's China revenue dropped amid intensifying competition from Huawei's Mate 60 series featuring domestically produced 7nm chips."),
    ("TSLA",  "neutral",  "Tesla FSD v12.5 enters wide release beta",
     "Tesla's Full Self-Driving software version 12.5 rolled out to all eligible vehicles in North America, featuring end-to-end neural network driving."),
    ("MSFT",  "negative", "Microsoft faces $29B IRS tax dispute",
     "The IRS issued a notice of proposed adjustment claiming Microsoft owes $28.9 billion in back taxes related to transfer pricing between 2004 and 2013."),
    ("GOOGL", "negative", "Google fined $2.4B by EU for search bias",
     "The European Court of Justice upheld a record antitrust fine against Google for systematically favouring its own comparison shopping service."),
    ("AMZN",  "negative", "Amazon warehouse workers in three states vote to unionize",
     "Workers at Amazon fulfilment centres in New York, Alabama, and California voted to join unions, potentially increasing labour costs by 15-20%."),
    ("NVDA",  "positive", "NVIDIA data center revenue surpasses $20B quarterly milestone",
     "NVIDIA's data center segment alone generated $22.6 billion in revenue, more than the company's total revenue just two years ago."),
    ("BTC-USD", "positive", "MicroStrategy purchases additional 12,000 BTC",
     "MicroStrategy added 12,000 Bitcoin to its treasury at an average price of $61,500, bringing total holdings to 226,000 BTC worth $14 billion."),
    ("ETH-USD", "positive", "Ethereum staking yield rises to 4.8% APR",
     "Ethereum's proof-of-stake staking rewards increased as network activity surged, attracting institutional validators seeking yield above Treasury rates."),
    ("AAPL",  "neutral",  "Apple WWDC 2025 developer conference dates announced",
     "Apple confirmed WWDC 2025 for June 9-13, with expectations focused on iOS 19 features and expanded on-device AI capabilities."),
    ("TSLA",  "negative", "Tesla Autopilot under investigation after highway accident",
     "NHTSA opened a preliminary investigation into a fatal crash involving a Tesla Model S with Autopilot engaged on a California highway."),
    ("MSFT",  "neutral",  "Microsoft to integrate OpenAI o3 model into Bing Search",
     "Microsoft announced plans to deploy OpenAI's latest reasoning model across Bing and Edge browser, though user adoption metrics remain unclear."),
    ("NVDA",  "negative", "NVIDIA insiders sell $700M in stock over past quarter",
     "SEC filings reveal NVIDIA executives and board members sold approximately $700 million worth of shares through pre-arranged 10b5-1 trading plans."),
    ("BTC-USD", "negative", "US Senator proposes cryptocurrency transaction tax",
     "A bipartisan Senate bill proposes a 0.1% tax on all cryptocurrency transactions above $1,000, which could significantly impact trading volumes."),
    ("ETH-USD", "negative", "Major DeFi protocol on Ethereum exploited for $45M",
     "A flash loan attack drained $45 million from a top-10 DeFi lending protocol, raising concerns about smart contract security on Ethereum."),
    ("AAPL",  "positive", "Apple services revenue reaches $25B quarterly record",
     "Apple's services segment including App Store, iCloud, and Apple TV+ posted record revenue of $25 billion, growing 15% year-over-year."),
    ("GOOGL", "neutral",  "Google announces Gemini 2.0 multimodal AI model",
     "Google DeepMind released Gemini 2.0 with enhanced reasoning and multimodal capabilities, though benchmark comparisons with GPT-5 remain inconclusive."),
    ("AMZN",  "positive", "Amazon same-day delivery expands to 30 new metro areas",
     "Amazon expanded its same-day delivery service to 30 additional US metropolitan areas, covering 90% of the US population within same-day reach."),
    ("NVDA",  "positive", "NVIDIA partners with major automakers for autonomous driving",
     "NVIDIA signed multi-year deals with Mercedes-Benz, BMW, and Toyota to supply its DRIVE Thor platform for Level 3+ autonomous driving systems."),
    ("BTC-USD", "neutral", "Bitcoin halving impact analysis — 6 months post-event",
     "Six months after the April 2024 halving, Bitcoin mining difficulty reached all-time highs while hash rate continues to grow despite reduced block rewards."),
    ("ETH-USD", "neutral", "Ethereum Foundation restructures leadership team",
     "The Ethereum Foundation announced organizational changes including new executive director appointment, focusing on protocol development and ecosystem grants."),
]

# Additional generic market articles
MARKET_ARTICLES = [
    ("neutral",  "Fed signals possible rate hold amid strong jobs data",
     "Federal Reserve officials indicated they may hold interest rates steady as the labour market remains resilient despite inflation pressures.",
     ["AAPL","MSFT","GOOGL","AMZN"]),
    ("positive", "S&P 500 posts third consecutive weekly gain on tech rally",
     "The index advanced 0.9% for the week, led by semiconductor and AI-linked stocks, as investors rotated back into growth names on cooling Treasury yields.",
     ["NVDA","MSFT","GOOGL","AAPL"]),
    ("negative", "US 10-year Treasury yield spikes above 4.6%",
     "Bond yields rose sharply after hotter-than-expected CPI data, pressuring growth stock valuations and sending the NASDAQ down 1.2% in afternoon trading.",
     ["TSLA","NVDA","AMZN","GOOGL"]),
    ("positive", "Tech sector leads market rally on AI optimism",
     "Major technology stocks rallied broadly as investor enthusiasm for artificial intelligence applications drove record capital inflows into tech-focused ETFs.",
     ["NVDA","MSFT","GOOGL","AMZN"]),
    ("negative", "Oil prices surge 8% on Middle East tensions",
     "Brent crude jumped to $92 per barrel as geopolitical tensions escalated, raising inflation concerns and putting pressure on consumer discretionary stocks.",
     ["AMZN","TSLA"]),
]


def generate_news_articles():
    """Generate 50+ sample news articles with varied timestamps."""
    articles = []
    base_time = datetime.now(timezone.utc)
    article_id = 1

    # Ticker-specific articles
    for ticker, sentiment, title, summary in HEADLINES:
        hours_ago = random.uniform(0.5, 168)  # up to 7 days ago
        published = base_time - timedelta(hours=hours_ago)
        source = random.choice(SOURCES)
        articles.append({
            "id": f"n{article_id:04d}",
            "source": source,
            "title": title,
            "summary": summary,
            "url": f"https://example.com/article/{article_id}",
            "published": published.isoformat(),
            "tickers": [ticker],
        })
        article_id += 1

    # Market-wide articles
    for sentiment, title, summary, tickers in MARKET_ARTICLES:
        hours_ago = random.uniform(0.5, 168)
        published = base_time - timedelta(hours=hours_ago)
        source = random.choice(SOURCES)
        articles.append({
            "id": f"n{article_id:04d}",
            "source": source,
            "title": title,
            "summary": summary,
            "url": f"https://example.com/article/{article_id}",
            "published": published.isoformat(),
            "tickers": tickers,
        })
        article_id += 1

    # Sort by published time descending
    articles.sort(key=lambda a: a["published"], reverse=True)
    return articles


# ---------------------------------------------------------------------------
# Stock prices
# ---------------------------------------------------------------------------
BASE_PRICES = {
    "AAPL": 189.75, "TSLA": 172.30, "MSFT": 415.20, "GOOGL": 175.10,
    "AMZN": 194.80, "NVDA": 924.60, "^GSPC": 5309.50, "^IXIC": 16780.20,
    "BTC-USD": 62400.00, "ETH-USD": 3020.00,
}

VOLATILITY = {
    "AAPL": 0.012, "TSLA": 0.025, "MSFT": 0.011, "GOOGL": 0.013,
    "AMZN": 0.014, "NVDA": 0.028, "^GSPC": 0.007, "^IXIC": 0.009,
    "BTC-USD": 0.035, "ETH-USD": 0.040,
}


def generate_stock_prices():
    """Generate 7 days of OHLCV price bars for all tickers."""
    prices = {}
    base_date = datetime.now(timezone.utc).replace(hour=16, minute=0, second=0, microsecond=0)

    for ticker, base_price in BASE_PRICES.items():
        bars = []
        price = base_price * random.uniform(0.95, 1.0)  # start slightly varied
        vol = VOLATILITY.get(ticker, 0.015)

        for day_offset in range(7, 0, -1):
            bar_date = base_date - timedelta(days=day_offset)
            daily_return = random.gauss(0.001, vol)
            open_price = price
            close_price = price * (1 + daily_return)
            high_price = max(open_price, close_price) * (1 + abs(random.gauss(0, vol * 0.5)))
            low_price = min(open_price, close_price) * (1 - abs(random.gauss(0, vol * 0.5)))
            volume = int(random.uniform(15_000_000, 95_000_000)) if not ticker.startswith("^") else 0

            bars.append({
                "date": bar_date.strftime("%Y-%m-%d"),
                "open": round(open_price, 2),
                "high": round(high_price, 2),
                "low": round(low_price, 2),
                "close": round(close_price, 2),
                "volume": volume,
                "change_pct": round(daily_return * 100, 2),
            })
            price = close_price

        prices[ticker] = bars

    return prices


def main():
    """Write sample data files to disk."""
    os.makedirs(SCRIPT_DIR, exist_ok=True)

    news_path = os.path.join(SCRIPT_DIR, "news_articles.json")
    prices_path = os.path.join(SCRIPT_DIR, "stock_prices.json")

    news = generate_news_articles()
    prices = generate_stock_prices()

    with open(news_path, "w", encoding="utf-8") as f:
        json.dump(news, f, indent=2, ensure_ascii=False)
    print(f"[sample-gen] Wrote {len(news)} articles → {news_path}")

    with open(prices_path, "w", encoding="utf-8") as f:
        json.dump(prices, f, indent=2, ensure_ascii=False)
    print(f"[sample-gen] Wrote {len(prices)} tickers (7 days each) → {prices_path}")


if __name__ == "__main__":
    main()
