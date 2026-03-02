"""
data/reddit_client.py - Reddit Sentiment & Mentions Engine
Scrapt r/wallstreetbets, r/stocks, r/investing, r/options
Kein PRAW nötig – nutzt Reddit's öffentliche JSON API.
Optional: REDDIT_CLIENT_ID + REDDIT_CLIENT_SECRET für höhere Rate Limits.
"""

import os, time, requests
from loguru import logger
from dotenv import load_dotenv
load_dotenv()

BULLISH_TERMS = {
    "bull","bullish","buy","buying","long","calls","moon","mooning","rocket",
    "gains","pump","pumping","upside","breakout","strong","support","accumulate",
    "undervalued","squeeze","short squeeze","earnings beat","upgrade","outperform",
    "overweight","price target raise","beat expectations",
}
BEARISH_TERMS = {
    "bear","bearish","sell","selling","short","puts","crash","crashing","dump",
    "dumping","downside","breakdown","weak","overvalued","bubble","correction",
    "recession","earnings miss","downgrade","underperform","underweight",
    "price target cut","bankruptcy","margin call","miss expectations","guidance cut",
}
SUBREDDITS = ["wallstreetbets","stocks","investing","options","stockmarket"]


class RedditClient:
    BASE_URL   = "https://www.reddit.com"
    OAUTH_URL  = "https://oauth.reddit.com"
    TOKEN_URL  = "https://www.reddit.com/api/v1/access_token"
    USER_AGENT = "AI-Analyst/3.1 (financial research)"

    def __init__(self):
        self.client_id     = os.getenv("REDDIT_CLIENT_ID","")
        self.client_secret = os.getenv("REDDIT_CLIENT_SECRET","")
        self._token        = None
        self._token_expiry = 0
        self.session       = requests.Session()
        self.session.headers.update({"User-Agent": self.USER_AGENT})

    def _get_oauth_token(self):
        if not (self.client_id and self.client_secret): return None
        if self._token and time.time() < self._token_expiry: return self._token
        try:
            resp = self.session.post(self.TOKEN_URL,
                auth=(self.client_id, self.client_secret),
                data={"grant_type": "client_credentials"}, timeout=6)
            if resp.status_code == 200:
                d = resp.json()
                self._token        = d.get("access_token")
                self._token_expiry = time.time() + d.get("expires_in",3600) - 120
                return self._token
        except Exception as e: logger.debug(f"[Reddit OAuth] {e}")
        return None

    def _auth_headers(self):
        t = self._get_oauth_token()
        return ({"Authorization":f"Bearer {t}","User-Agent":self.USER_AGENT}
                if t else {"User-Agent":self.USER_AGENT})

    def _base_url(self):
        return self.OAUTH_URL if self._get_oauth_token() else self.BASE_URL

    def get_ticker_sentiment(self, ticker: str, limit_per_sub: int = 15) -> dict:
        all_posts, sub_counts = [], {}
        for sub in SUBREDDITS:
            posts = self._search_subreddit(sub, ticker, limit_per_sub)
            sub_counts[sub] = len(posts)
            all_posts.extend(posts)

        if not all_posts:
            return {"mentions":0,"sentiment":0.0,"sentiment_label":"Kein Signal",
                    "bullish_pct":0.0,"bearish_pct":0.0,"top_posts":[],"subreddit_breakdown":sub_counts}

        bullish = bearish = 0
        for post in all_posts:
            text = (post["title"] + " " + post["selftext"]).lower()
            b = sum(1 for w in BULLISH_TERMS if w in text)
            s = sum(1 for w in BEARISH_TERMS if w in text)
            post["post_sentiment"] = round((b-s)/max(b+s,1), 2)
            if b > s:   bullish += 1
            elif s > b: bearish += 1

        total   = len(all_posts)
        overall = (bullish - bearish) / total

        return {
            "mentions":           total,
            "sentiment":          round(overall, 3),
            "sentiment_label":    ("Bullish 🚀" if overall>0.15
                                   else "Bearish 🐻" if overall<-0.15
                                   else "Neutral 😐"),
            "bullish_pct":        round(bullish/total*100, 1),
            "bearish_pct":        round(bearish/total*100, 1),
            "top_posts":          sorted(all_posts, key=lambda x:x.get("score",0), reverse=True)[:5],
            "subreddit_breakdown": sub_counts,
        }

    def _search_subreddit(self, sub, ticker, limit):
        try:
            resp = self.session.get(
                f"{self._base_url()}/r/{sub}/search.json",
                params={"q":ticker,"sort":"hot","limit":limit,"restrict_sr":"true","t":"week"},
                headers=self._auth_headers(), timeout=7)
            if resp.status_code != 200: return []
            posts = []
            for item in resp.json().get("data",{}).get("children",[]):
                p = item["data"]
                posts.append({
                    "title":        p.get("title",""),
                    "selftext":     p.get("selftext","")[:300],
                    "score":        p.get("score",0),
                    "upvote_ratio": round(p.get("upvote_ratio",0.5),2),
                    "num_comments": p.get("num_comments",0),
                    "subreddit":    f"r/{sub}",
                    "url":          f"https://reddit.com{p.get('permalink','')}",
                    "created_utc":  p.get("created_utc",0),
                })
            return posts
        except Exception as e:
            logger.debug(f"[Reddit] r/{sub}/'{ticker}': {e}")
            return []


_instance = None
def get_reddit_client() -> RedditClient:
    global _instance
    if _instance is None: _instance = RedditClient()
    return _instance