import requests
import os
import json
from dotenv import load_dotenv
from datetime import datetime, timedelta, timezone
from google.cloud.exceptions import NotFound
from google.cloud import bigquery

# Variables
load_dotenv() # Loads environmental variable from .env
target_user_id = '96951800' # FC Barcelona twitter account
current_time = datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ")
seven_days_back = (datetime.now() - timedelta(days = 7)).strftime("%Y-%m-%dT%H:%M:%SZ")
bearer_token = os.getenv('TWITTER_BEARER_TOKEN')

# Create class to handle bearer token authentication within API calls
class BearerAuth(requests.auth.AuthBase):
    def __init__(self, token):
        self.token = token
    def __call__(self, r):
        r.headers["authorization"] = "Bearer " + self.token
        return r

# Google Bigquery Setup
client = bigquery.Client()

# Bigquery portion -- check for existing items and create missing ones
# This requires installation of the google cloud CLI and configuration of an 
# Application Default Credentials (ADC) file

def bq_create_dataset_if_not_exists(client, dataset_id):

    # The dataset_id = 'your-project.your_dataset'
    try:
        client.get_dataset(dataset_id)  
        print('Dataset ' + f'{dataset_id}' + 'already exists')

    except NotFound:

        # Construct a full Dataset object to send to the API.
        dataset = bigquery.Dataset(dataset_id)

        # Specify the geographic location where the dataset should reside.
        dataset.location = 'US'

        # Send the dataset to the API for creation.
        # Raises google.api_core.exceptions.Conflict if the Dataset already
        # exists within the project.
        dataset = client.create_dataset(dataset)  
        target_project = client.project
        
        print(f'Created dataset {target_project}.{dataset_id}')

def bq_create_table_if_not_exists(client, table_id, schema):

    # The table_id = 'your-project.your_dataset.your_table'
    try:
        client.get_table(table_id)  
        print(f'Table {table_id} already exists')

    except NotFound:
        
        table = bigquery.Table(table_id, schema=schema)
        table = client.create_table(table)  
        print(
            f'Created table {table.project}.{table.dataset_id}.{table.table_id}'
        )

def table_insert_rows(client, table_id, rows_to_insert):

    table = client.get_table(table_id)  
    errors = client.insert_rows(table, rows_to_insert)  
    if errors == []:
        print(f'New rows have been added to {table_id}')
    else:
        print(f'New rows not added to {table_id}')

# Create schema for each table

account_interactions_schema = [
    bigquery.SchemaField('tweet_id', 'INTEGER')
    , bigquery.SchemaField('tweet_author_id', 'INTEGER')
    , bigquery.SchemaField('created_at', 'DATETIME')
    , bigquery.SchemaField('like_count', 'INTEGER')
    , bigquery.SchemaField('quote_count', 'INTEGER')
    , bigquery.SchemaField('reply_count', 'INTEGER')
    , bigquery.SchemaField('retweet_count', 'INTEGER')
    , bigquery.SchemaField('reply_settings', 'STRING')
    , bigquery.SchemaField('tweet_text', 'STRING')
    , bigquery.SchemaField('tweet_url', 'STRING')
    , bigquery.SchemaField('interaction_type', 'STRING')
]

followed_users_schema = [
    bigquery.SchemaField('username', 'STRING')
    , bigquery.SchemaField('user_id', 'INTEGER')
    , bigquery.SchemaField('user_name', 'STRING')
    , bigquery.SchemaField('followers_count', 'INTEGER')
    , bigquery.SchemaField('following_count', 'INTEGER')
    , bigquery.SchemaField('tweet_count', 'INTEGER')
    , bigquery.SchemaField('listed_count', 'INTEGER')
    , bigquery.SchemaField('user_created_at', 'DATETIME')
    , bigquery.SchemaField('is_verified', 'BOOL')
    , bigquery.SchemaField('location', 'STRING')
]

following_users_schema = [
    bigquery.SchemaField('username', 'STRING')
    , bigquery.SchemaField('user_id', 'INTEGER')
    , bigquery.SchemaField('user_name', 'STRING')
    , bigquery.SchemaField('followers_count', 'INTEGER')
    , bigquery.SchemaField('following_count', 'INTEGER')
    , bigquery.SchemaField('tweet_count', 'INTEGER')
    , bigquery.SchemaField('listed_count', 'INTEGER')
    , bigquery.SchemaField('user_created_at', 'DATETIME')
    , bigquery.SchemaField('is_verified', 'BOOL')
    , bigquery.SchemaField('location', 'STRING')
]

past_tweets_schema = [
    bigquery.SchemaField('tweet_id', 'INTEGER')
    , bigquery.SchemaField('created_at', 'DATETIME')
    , bigquery.SchemaField('like_count', 'INTEGER')
    , bigquery.SchemaField('quote_count', 'INTEGER')
    , bigquery.SchemaField('reply_count', 'INTEGER')
    , bigquery.SchemaField('retweet_count', 'INTEGER')
    , bigquery.SchemaField('reply_settings', 'STRING')
    , bigquery.SchemaField('tweet_text', 'STRING')
    , bigquery.SchemaField('tweet_url', 'STRING')
]

tweet_interacting_users_schema = [
    bigquery.SchemaField('tweet_id', 'INTEGER')
    , bigquery.SchemaField('interaction_type', 'STRING')
    , bigquery.SchemaField('username', 'STRING')
    , bigquery.SchemaField('user_id', 'INTEGER')
    , bigquery.SchemaField('user_name', 'STRING')
    , bigquery.SchemaField('followers_count', 'INTEGER')
    , bigquery.SchemaField('following_count', 'INTEGER')
    , bigquery.SchemaField('tweet_count', 'INTEGER')
    , bigquery.SchemaField('listed_count', 'INTEGER')
    , bigquery.SchemaField('is_verified', 'BOOL')
    , bigquery.SchemaField('location', 'STRING')    
]       

# Create dataset if it does not exist
bq_create_dataset_if_not_exists(client, 'akdm-assessment.barca_twitter_data')

# Create tables if they don't exist
bq_create_table_if_not_exists(
    client
    , 'akdm-assessment.barca_twitter_data.account_interactions'
    , account_interactions_schema
)

bq_create_table_if_not_exists(
    client
    , 'akdm-assessment.barca_twitter_data.followed_users'
    , followed_users_schema
)

bq_create_table_if_not_exists(
    client
    , 'akdm-assessment.barca_twitter_data.following_users'
    , following_users_schema
)

bq_create_table_if_not_exists(
    client
    , 'akdm-assessment.barca_twitter_data.past_tweets'
    , past_tweets_schema
)

bq_create_table_if_not_exists(
    client
    , 'akdm-assessment.barca_twitter_data.tweet_interacting_users'
    , tweet_interacting_users_schema
)

# Capture past seven days of tweets from FC Barcelona
try:
    past_tweet_response = requests.get(
        url = 
            'https://api.twitter.com/2/users/' 
            + f'{target_user_id}' 
            + '/tweets?start_time=' 
            + f'{seven_days_back}'  
            + '&end_time=' 
            + f'{current_time}'
            + '&tweet.fields=id,text,created_at,public_metrics,referenced_tweets,reply_settings'
        , auth = BearerAuth(f'{bearer_token}')
    )
    past_tweet_response.raise_for_status
    past_tweet_data = json.loads(past_tweet_response.text)

except Exception as past_tweet_e:
    print(f'{past_tweet_e.past_tweet_response.status_code} {past_tweet_e.past_tweet_response.reason}')

# Flatten past tweet data
flat_past_tweets = []

for tweet in past_tweet_data['data']:

    # Position of URL within tweet text
    url_index = int((tweet.get('text')).find('http')) 

    flat_past_tweets.append({
        'tweet_id': tweet.get('id')
        , 'created_at': (tweet.get('created_at'))[:-1]
        , 'like_count': (tweet.get('public_metrics', {})).get('like_count')
        , 'quote_count': (tweet.get('public_metrics', {})).get('quote_count')
        , 'reply_count': (tweet.get('public_metrics', {})).get('reply_count')
        , 'retweet_count': (tweet.get('public_metrics', {})).get('retweet_count')
        , 'reply_settings': tweet.get('reply_settings')
        , 'tweet_text': (tweet.get('text'))[:url_index-1]
        , 'tweet_url': (tweet.get('text'))[url_index:]
    })

# Create empty dict for adding tweet interactions
flat_tweet_interacting_users = []

# Create function to gather the info of users who interacted with a tweet
def get_tweet_interacting_users(tweet_data, interaction_type, api_endpoint):

    # Create function to flatten users and add to interaction dict
    def flatten_user_interaction(interaction_data, interaction_type, origin_tweet_id):

        for interacting_user in interaction_data:

            flat_tweet_interacting_users.append({
                'tweet_id': f'{origin_tweet_id}'
                , 'interaction_type': f'{interaction_type}'
                , 'username': interacting_user.get('username')
                , 'user_id': interacting_user.get('id')
                , 'user_name': interacting_user.get('name')
                , 'followers_count': (interacting_user.get('public_metrics', {})).get('followers_count')
                , 'following_count': (interacting_user.get('public_metrics', {})).get('following_count')
                , 'tweet_count': (interacting_user.get('public_metrics', {})).get('tweet_count')
                , 'listed_count': (interacting_user.get('public_metrics', {})).get('listed_count')
                , 'is_verified': interacting_user.get('verified')
                , 'location': interacting_user.get('location')
            })       

    # Loop through tweets to get information about interacting users
    for past_tweet in tweet_data:

        iteration_tweet_id = str(past_tweet['tweet_id']) # Define origin tweet for iteration

        if 'tweet_id' in past_tweet and 'pagination_token' not in past_tweet:

            try:
                
                interaction_response = requests.get(
                    url =
                        'https://api.twitter.com/2/tweets/'
                        + iteration_tweet_id
                        + '/'
                        + f'{api_endpoint}'
                        + '?user.fields=id,name,username,created_at,location,public_metrics,verified'
                    , auth = BearerAuth(f'{bearer_token}')
                )

                interaction_response.raise_for_status
                interaction_data = json.loads(interaction_response.text)

            except Exception as interaction_e:

                print(f'{interaction_e.interaction_response.status_code} {interaction_e.interaction_response.reason}')

            # Flatten results and append to final interaction dict            
            flatten_user_interaction(interaction_data['data'], interaction_type, iteration_tweet_id)

            # When a next token is present extend the tweet list with the next_token
            if (past_tweet.get('meta', {})).get('next_token') != None:

                # Create empty dict and append next_token for a paginated api call at end of past_tweet loop

                response_pagination = []

                response_pagination.append({
                    'tweet_id': iteration_tweet_id
                    , 'pagination_token': (past_tweet.get('meta', {})).get('next_token')
                })

                # Extend currently iterating list with pagination
                tweet_data.extend(response_pagination)

        # When the list object is a next_token instead of a tweet perform an alternate api call
        elif 'pagination_token' in past_tweet:

            try: 

                pagination_response = requests.get(
                    url = 
                        'https://api.twitter.com/2/tweets/'
                        + str(past_tweet['tweet_id'])
                        + '/'
                        + f'{api_endpoint}'
                        + '?user.fields=id,name,username,created_at,location,public_metrics,verified'
                        + '&pagination_token='
                        + str(past_tweet['pagination_token'])
                    , auth = BearerAuth(f'{bearer_token}')
                )

                pagination_response.raise_for_status
                pagination_data = json.loads(pagination_response.text)

            except Exception as pagination_e:

                print(f'{pagination_e.pagination_response.status_code} {pagination_e.pagination_response.reason}')

            # Flatten results and append to final interaction dict            
            flatten_user_interaction(pagination_data['data'], interaction_type, iteration_tweet_id)

# Use function to get interacting users of multiple types
get_tweet_interacting_users(flat_past_tweets, 'like tweet', 'liking_users')
get_tweet_interacting_users(flat_past_tweets, 'quote tweet', 'quote_tweets')
get_tweet_interacting_users(flat_past_tweets, 'retweet', 'retweeted_by')

# Create empty dict for adding account interactions
flat_account_interactions = []

# Capture last 100 likes by target account
try:
    account_interaction_response = requests.get(
        url =
            'https://api.twitter.com/2/users/'
            + f'{target_user_id}'
            + '/'
            + 'liked_tweets'
            + '?tweet.fields=author_id,created_at,id,public_metrics,reply_settings,text'
            + '&max_results=100'
        , auth = BearerAuth(f'{bearer_token}')
    )
    account_interaction_response.raise_for_status
    account_interaction_data = json.loads(account_interaction_response.text)

except Exception as account_interaction_e:
    print(f'{account_interaction_e.interaction_response.status_code} {account_interaction_e.interaction_response.reason}')

# Flatten results and append to final account interaction dict            
for account_interaction in account_interaction_data['data']:

    # Position of URL within tweet text
    url_index = int((account_interaction.get('text')).find('http'))             

    # Flatten and append
    flat_account_interactions.append({
        'tweet_text': (account_interaction.get('text'))[:url_index-1]
        , 'tweet_url': (account_interaction.get('text'))[url_index:]
        , 'reply_settings': account_interaction.get('reply_settings')  
        , 'tweet_id': account_interaction.get('id')
        , 'tweet_author_id': account_interaction.get('author_id')
        , 'created_at': (account_interaction.get('created_at'))[:-1]
        , 'like_count': (account_interaction.get('public_metrics', {})).get('like_count')
        , 'quote_count': (account_interaction.get('public_metrics', {})).get('quote_count')
        , 'reply_count': (account_interaction.get('public_metrics', {})).get('reply_count')
        , 'retweet_count': (account_interaction.get('public_metrics', {})).get('retweet_count')
        , 'interaction_type': 'liked tweet'
    })

# Get follow type data -- intentionally limited to 1000 to avoid rate limits

# Create empty dict for adding following users
flat_following_users = []

# Create empty dict for adding followed users
flat_followed_users = []

# Get data
def get_follows_data(target_dict, api_endpoint, target_user_id):

    try:
        
        follow_response = requests.get(
            url =
                'https://api.twitter.com/2/users/'
                + f'{target_user_id}'
                + '/'
                + f'{api_endpoint}'
                + '?user.fields=id,name,username,created_at,location,public_metrics,verified'
                + '&max_results=1000'
            , auth = BearerAuth(f'{bearer_token}')
        )

        follow_response.raise_for_status
        follow_data = json.loads(follow_response.text)

    except Exception as follow_e:

        print(f'{follow_e.interaction_response.status_code} {follow_e.interaction_response.reason}')

    # Flatten results and append to final interaction dict            
    for follow in follow_data['data']:           

        target_dict.append({
            'username': follow.get('username')
            , 'user_id': follow.get('id')
            , 'user_name': follow.get('name')
            , 'followers_count': (follow.get('public_metrics', {})).get('followers_count')
            , 'following_count': (follow.get('public_metrics', {})).get('following_count')
            , 'tweet_count': (follow.get('public_metrics', {})).get('tweet_count')
            , 'listed_count': (follow.get('public_metrics', {})).get('listed_count')
            , 'user_created_at': (follow.get('created_at'))[:-1]
            , 'is_verified': follow.get('verified')
            , 'location': follow.get('location')
        })       

get_follows_data(flat_following_users, 'following', target_user_id)
get_follows_data(flat_followed_users, 'followers', target_user_id)      

# Insert flat data
table_insert_rows(client, 'akdm-assessment.barca_twitter_data.account_interactions', flat_account_interactions)
table_insert_rows(client, 'akdm-assessment.barca_twitter_data.followed_users', flat_followed_users)
table_insert_rows(client, 'akdm-assessment.barca_twitter_data.following_users', flat_following_users)
table_insert_rows(client, 'akdm-assessment.barca_twitter_data.past_tweets', flat_past_tweets)
table_insert_rows(client, 'akdm-assessment.barca_twitter_data.tweet_interacting_users', flat_tweet_interacting_users)