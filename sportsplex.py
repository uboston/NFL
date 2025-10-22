import os
import json
import boto3
from datetime import datetime
import requests
from typing import List, Dict

# MLB Teams
MLB_TEAMS = [
    "Arizona Diamondbacks", "Atlanta Braves", "Baltimore Orioles", "Boston Red Sox",
    "Chicago White Sox", "Chicago Cubs", "Cincinnati Reds", "Cleveland Guardians",
    "Colorado Rockies", "Detroit Tigers", "Houston Astros", "Kansas City Royals",
    "Los Angeles Angels", "Los Angeles Dodgers", "Miami Marlins", "Milwaukee Brewers",
    "Minnesota Twins", "New York Yankees", "New York Mets", "Oakland Athletics",
    "Philadelphia Phillies", "Pittsburgh Pirates", "San Diego Padres", "San Francisco Giants",
    "Seattle Mariners", "St. Louis Cardinals", "Tampa Bay Rays", "Texas Rangers",
    "Toronto Blue Jays", "Washington Nationals"
]

class MLBNewsScraper:
    def __init__(self, perplexity_api_key: str, aws_access_key: str, 
                 aws_secret_key: str, bucket_name: str, region: str = 'us-east-1'):
        """
        Initialize the scraper with API credentials
        
        Args:
            perplexity_api_key: Perplexity API key
            aws_access_key: AWS access key
            aws_secret_key: AWS secret key
            bucket_name: S3 bucket name
            region: AWS region (default: us-east-1)
        """
        self.perplexity_api_key = perplexity_api_key
        self.bucket_name = bucket_name
        
        # Initialize S3 client
        self.s3_client = boto3.client(
            's3',
            aws_access_key_id=aws_access_key,
            aws_secret_access_key=aws_secret_key,
            region_name=region
        )
        
        self.perplexity_url = "https://api.perplexity.ai/chat/completions"
    
    def query_perplexity(self, team_name: str) -> Dict:
        """
        Query Perplexity API for news about a specific MLB team
        
        Args:
            team_name: Name of the MLB team
            
        Returns:
            Dictionary containing the API response
        """
        headers = {
            "Authorization": f"Bearer {self.perplexity_api_key}",
            "Content-Type": "application/json"
        }
        
        payload = {
            "model": "llama-3.1-sonar-small-128k-online",
            "messages": [
                {
                    "role": "system",
                    "content": "You are a helpful assistant that finds accurate recent news articles. Return exactly 5 recent news articles with title, source, date, and URL."
                },
                {
                    "role": "user",
                    "content": f"Find 5 recent news articles about the {team_name} from within the last 24 hours. For each article provide: title, source, publication date, brief summary, and URL."
                }
            ],
            "max_tokens": 2000,
            "temperature": 0.2,
            "return_citations": True,
            "return_images": False
        }
        
        try:
            response = requests.post(self.perplexity_url, json=payload, headers=headers)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"Error querying Perplexity API for {team_name}: {e}")
            return None
    
    def upload_to_s3(self, team_name: str, data: Dict) -> bool:
        """
        Upload news data to S3 bucket
        
        Args:
            team_name: Name of the MLB team
            data: News data to upload
            
        Returns:
            True if successful, False otherwise
        """
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        team_slug = team_name.lower().replace(" ", "_")
        file_name = f"mlb_news/{team_slug}/{timestamp}.json"
        
        try:
            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=file_name,
                Body=json.dumps(data, indent=2),
                ContentType='application/json'
            )
            print(f"✓ Uploaded news for {team_name} to s3://{self.bucket_name}/{file_name}")
            return True
        except Exception as e:
            print(f"✗ Error uploading to S3 for {team_name}: {e}")
            return False
    
    def process_all_teams(self):
        """
        Process all MLB teams: query Perplexity and upload to S3
        """
        print(f"Starting MLB news scraper for {len(MLB_TEAMS)} teams...\n")
        
        successful = 0
        failed = 0
        
        for i, team in enumerate(MLB_TEAMS, 1):
            print(f"[{i}/{len(MLB_TEAMS)}] Processing {team}...")
            
            # Query Perplexity API
            news_data = self.query_perplexity(team)
            
            if news_data:
                # Upload to S3
                if self.upload_to_s3(team, news_data):
                    successful += 1
                else:
                    failed += 1
            else:
                failed += 1
            
            print()  # Empty line for readability
        
        print(f"\n{'='*50}")
        print(f"Processing complete!")
        print(f"Successful: {successful}")
        print(f"Failed: {failed}")
        print(f"{'='*50}")


def main():
    """
    Main function to run the scraper
    """
    # Load credentials from environment variables
    PERPLEXITY_API_KEY = os.getenv('PERPLEXITY_API_KEY')
    AWS_ACCESS_KEY = os.getenv('AWS_ACCESS_KEY_ID')
    AWS_SECRET_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
    S3_BUCKET_NAME = os.getenv('S3_BUCKET_NAME')
    AWS_REGION = os.getenv('AWS_REGION', 'us-east-1')
    
    # Validate credentials
    if not all([PERPLEXITY_API_KEY, AWS_ACCESS_KEY, AWS_SECRET_KEY, S3_BUCKET_NAME]):
        print("Error: Missing required environment variables.")
        print("Please set: PERPLEXITY_API_KEY, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, S3_BUCKET_NAME")
        return
    
    # Initialize and run scraper
    scraper = MLBNewsScraper(
        perplexity_api_key=PERPLEXITY_API_KEY,
        aws_access_key=AWS_ACCESS_KEY,
        aws_secret_key=AWS_SECRET_KEY,
        bucket_name=S3_BUCKET_NAME,
        region=AWS_REGION
    )
    
    scraper.process_all_teams()


if __name__ == "__main__":
    main()