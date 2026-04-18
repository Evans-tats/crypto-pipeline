import json
import logging
import os
import time

import praw

from kafka import KafkaProducer


logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
log = logging.getLogger(__name__)
 

KAFKA_BOOTSTRAP_SERVERS = "localhost:29092"
KAFKA_TOPIC = "sentiment_analysis"

SUBREDDITS = ["CryptoCurrency", "Bitcoin", "Ethereum", "Solana"]
SYMBOL_KEYWORDS = {
    "BTCUSDT": ["bitcoin", "btc", "xbt"],
    "ETHUSDT": ["ethereum", "eth"],
    "SOLUSDT": ["solana", "sol"],
}


def make_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda x: json.dumps(x).encode("utf-8"),
        key_serializer=lambda x: x.encode("utf-8"),
        acks="all",
        retries=5,
    )

def make_reddit() -> praw.Reddit:
    return praw.Reddit(
        client_id=os.environ.get("REDDIT_CLIENT_ID"),
        client_secret=os.environ.get("REDDIT_CLIENT_SECRET"),
        user_agent=os.environ.get("REDDIT_USER_AGENT", "crypto-sentiment-producer/1.0"),
    )
def detect_symbols() -> praw.reddit:
    text_lower = text.lower()
    return [
        symbol
        for symbol, keywords in SYMBOL_KEYWORDS.items()
        if any(kw in text_lower for kw in keywords)
    ]

def submission_to_record(submission: praw.models.Submission, symbols: list[str]) -> dict:
    return {
        "post_id": submission.id,
        "subreddit": submission.subreddit.display_name,
        "title": submission.title,
        "score": submission.score,
        "upvote_ratio": submission.upvote_ratio,
        "num_comments": submission.num_comments,
        "symbols": symbols,
        "url": submission.url,
        "created_utc": int(submission.created_utc),
        "ingested_at": int(time.time() * 1000),
    }
def main() -> None:
    producer = make_producer()
    reddit = make_reddit()
    sub_stream = "+".join(SUBREDDITS)
    log.info("Streaming r/%s into %s", sub_stream, TOPIC)

    while True:
        try:
            subreddit = reddit.subreddit(sub_stream)
            for submission in subreddit.stream.submissions(skip_existing=True):
                full_text = f"{submission.title} {submission.selftext}"
                symbols = detect_symbols(full_text)
                if not symbols:
                    continue

                record = submission_to_record(submission, symbols)
                for symbol in symbols:
                    producer.send(TOPIC, key=symbol, value=record)

                log.info(
                    "Post '%s...' → symbols %s",
                    submission.title[:40],
                    symbols,
                )
                producer.flush()

        except Exception as exc:
            log.error("Stream error: %s. Reconnecting in 30s", exc, exc_info=True)
            time.sleep(30)

if __name__ == "__main__":
    main()
