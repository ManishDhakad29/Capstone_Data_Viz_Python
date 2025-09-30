# Capstone: Retrieving, Processing, and Visualizing Data with Python

# 1. IMPORTS - Necessary libraries for the ETL process
import tweepy
import sqlite3
import pandas as pd
from nltk.sentiment.vader import SentimentIntensityAnalyzer
import random
from datetime import datetime

# --- PLACEHOLDERS for real data extraction ---
def get_tweets(query, count=10000):
    """Placeholder function to simulate retrieving and cleaning social media data."""
    print("--- 1. Retrieving Data (Simulated) ---")
    
    # Simulating 10,000 tweets for the Capstone objective
    data = {'tweet_id': range(10000, 20000),
            'text': [
                "Vaccine rollout is progressing well! Very positive news.", 
                "Still skeptical about the long-term effects. #doubt", 
                "Neutral update on new vaccine policy.",
                "Feeling great after the booster shot! Best decision ever.",
                "Why is the government not clarifying the side effects? I am worried.",
                "Vaccine availability is smooth in my city."
            ] * 2000 + ["Another random neutral tweet."]*2000,
            'user_location': [random.choice(['Mumbai', 'Delhi', 'Bangalore', 'Chennai', 'Other']) for _ in range(10000)],
            'created_at': [datetime.now().strftime("%Y-%m-%d %H:%M:%S")] * 10000}
            
    df = pd.DataFrame(data)
    print(f"Simulated and retrieved {len(df)} records.")
    return df

# --- 2. TRANSFORMATION (Cleaning and Analysis) ---
def transform_data(df):
    """Performs text cleaning and sentiment analysis on the data."""
    print("--- 2. Transforming Data ---")
    
    # Sentiment Analysis using NLTK VADER
    sid = SentimentIntensityAnalyzer()
    df['scores'] = df['text'].apply(lambda x: sid.polarity_scores(x))
    df['sentiment'] = df['scores'].apply(lambda x: 'Positive' if x['compound'] >= 0.05 else ('Negative' if x['compound'] <= -0.05 else 'Neutral'))
    
    # Selecting final columns for database loading
    final_df = df[['tweet_id', 'text', 'user_location', 'sentiment', 'created_at']]
    return final_df

# --- 3. LOADING (Storing in SQLite) ---
def load_to_db(df):
    """Creates an SQLite database and loads the transformed data into a table."""
    print("--- 3. Loading Data to SQLite ---")
    conn = sqlite3.connect('vaccine_data.sqlite')
    
    # Loading data into the 'Tweets' table
    df.to_sql('Tweets', conn, if_exists='replace', index=False)
    
    # Simple check to confirm data is loaded
    check_count = pd.read_sql_query("SELECT COUNT(*) FROM Tweets", conn).iloc[0, 0]
    print(f"Successfully loaded {check_count} records into vaccine_data.sqlite")
    
    conn.close()

# --- 4. VISUALIZATION (Placeholder) ---
def visualize_data():
    """Placeholder for the final Plotly visualization step."""
    print("--- 4. Visualization: Data is ready in vaccine_data.sqlite. Use Plotly/Flask to visualize trends. ---")

# --- MAIN EXECUTION ---
if __name__ == "__main__":
    # The topic for your capstone analysis
    TOPIC_QUERY = 'COVID-19 vaccine OR vaccination' 
    
    # Start the ETL pipeline
    raw_df = get_tweets(TOPIC_QUERY)
    processed_df = transform_data(raw_df)
    load_to_db(processed_df)
    visualize_data()
