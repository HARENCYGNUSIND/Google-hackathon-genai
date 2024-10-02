import pandas as pd
import schedule
import time
import requests
from jinja2 import Template

# Load the dataset
def load_dataset(file_path):
    df = pd.read_csv(file_path, parse_dates=['booking_date'])  # Assuming dataset has a 'booking_date' column
    df['month'] = df['booking_date'].dt.to_period('M')  # Extract year-month for monthly grouping
    return df

# Categorize users based on booking style (e.g., total bookings or revenue)
def categorize_users(df):
    # Group by user_id to calculate metrics
    user_behavior = df.groupby('user_id').agg({
        'total_revenue': 'sum',
        'booking_date': 'count'  # Total bookings per user
    }).rename(columns={'booking_date': 'num_bookings'})

    # Determine thresholds
    high_threshold = user_behavior['num_bookings'].quantile(0.75)
    medium_threshold = user_behavior['num_bookings'].quantile(0.50)
    
    def assign_category(row):
        if row['num_bookings'] >= high_threshold:
            return 'High'
        elif row['num_bookings'] >= medium_threshold:
            return 'Medium'
        else:
            return 'Low'
    
    user_behavior['category'] = user_behavior.apply(assign_category, axis=1)
    
    return user_behavior

# Analyze off-season performance on a monthly basis
def analyze_offseason_performance(df):
    # Group by month and calculate monthly metrics
    monthly_performance = df.groupby('month').agg({
        'total_revenue': 'sum',
        'user_id': 'count'
    }).rename(columns={'user_id': 'num_bookings'})
    
    baseline_revenue = monthly_performance['total_revenue'].mean()
    baseline_bookings = monthly_performance['num_bookings'].mean()
    
    monthly_performance['revenue_drop'] = (baseline_revenue - monthly_performance['total_revenue']) / baseline_revenue * 100
    monthly_performance['bookings_drop'] = (baseline_bookings - monthly_performance['num_bookings']) / baseline_bookings * 100
    
    # Categorize off-season based on percentage drops
    monthly_performance['season_category'] = monthly_performance.apply(
        lambda row: 'Low' if row['revenue_drop'] > 20 else 'Medium' if row['revenue_drop'] > 10 else 'High', axis=1
    )
    
    return monthly_performance

# Generate content based on user category and off-season category
def generate_content(user_id, user_category, season_category):
    if user_category == 'High':
        if season_category == 'Low':
            discount = "40% OFF"
        elif season_category == 'Medium':
            discount = "30% OFF"
        else:
            discount = "20% OFF"
    elif user_category == 'Medium':
        if season_category == 'Low':
            discount = "30% OFF"
        elif season_category == 'Medium':
            discount = "20% OFF"
        else:
            discount = "10% OFF"
    else:  # Low user category
        if season_category == 'Low':
            discount = "20% OFF"
        elif season_category == 'Medium':
            discount = "15% OFF"
        else:
            discount = "5% OFF"
    
    # Content template
    template = Template("""
        <h1>Special Offer for You, User {{ user_id }}!</h1>
        <p>As a valued {{ user_category }} customer, we are excited to offer you {{ discount }} due to {{ season_category }} season performance.</p>
    """)
    
    return template.render(user_id=user_id, user_category=user_category, discount=discount, season_category=season_category)

# API call to send email campaign
def send_email_via_api(user_id, email_content):
    url = "https://your-mailchimp-api.com/send-email"
    headers = {
        "Authorization": "Bearer YOUR_MAILCHIMP_API_KEY",
        "Content-Type": "application/json"
    }
    data = {
        "to": f"user_{user_id}@example.com",
        "subject": "Special Offer Just for You!",
        "content": email_content
    }
    response = requests.post(url, headers=headers, json=data)
    
    if response.status_code == 200:
        print(f"Email sent to user {user_id} successfully!")
    else:
        print(f"Failed to send email to user {user_id}: {response.status_code}")

# Automated content generation and sending
def automate_campaigns(file_path):
    df = load_dataset(file_path)
    
    # Categorize users based on booking style
    user_behavior = categorize_users(df)
    
    # Analyze off-season performance
    monthly_performance = analyze_offseason_performance(df)
    
    # Get the latest month and its season category
    latest_month = monthly_performance.index[-1]
    season_category = monthly_performance.loc[latest_month, 'season_category']
    
    # Generate and send email for each user based on their category and season
    for user_id, row in user_behavior.iterrows():
        user_category = row['category']
        email_content = generate_content(user_id, user_category, season_category)
        send_email_via_api(user_id, email_content)

# Schedule to run monthly at the beginning of each month
file_path = r"C:\Users\hp\Downloads\updated_hotel_booking_dataset.csv"
schedule.every().month.at("00:00").do(automate_campaigns, file_path)

# Start the schedule
while True:
    schedule.run_pending()
    time.sleep(60)
