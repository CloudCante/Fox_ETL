from datetime import datetime, timedelta

def get_iso_week_id(date_obj):
    """Convert date to ISO week format: 2025-W23"""
    year, week, _ = date_obj.isocalendar()
    return f"{year}-W{week:02d}"

def get_week_date_range(week_id):
    """Get start and end dates for an ISO week"""
    year, week = week_id.split('-W')
    year = int(year)
    week = int(week)
    
    jan4 = datetime(year, 1, 4)
    week_start = jan4 - timedelta(days=jan4.weekday()) + timedelta(weeks=week-1)
    week_end = week_start + timedelta(days=6)
    
    return week_start.date(), week_end.date()

def test_week_boundaries():
    """Test week boundaries for different dates"""
    print("Testing Week Boundaries")
    print("=" * 50)
    
    test_dates = [
        datetime(2025, 5, 14).date(),  # Wednesday
        datetime(2025, 5, 12).date(),  # Monday
        datetime(2025, 5, 18).date(),  # Sunday
        datetime(2025, 6, 15).date(),  # Sunday
        datetime(2025, 6, 13).date(),  # Friday
    ]
    
    for test_date in test_dates:
        week_id = get_iso_week_id(test_date)
        week_start, week_end = get_week_date_range(week_id)
        
        print(f"\nDate: {test_date.strftime('%Y-%m-%d')} ({test_date.strftime('%A')})")
        print(f"  Week ID: {week_id}")
        print(f"  Week Range: {week_start.strftime('%Y-%m-%d')} ({week_start.strftime('%A')}) to {week_end.strftime('%Y-%m-%d')} ({week_end.strftime('%A')})")
        
        if week_start <= test_date <= week_end:
            print(f"Date is within week range")
        else:
            print(f"Date is NOT within week range")

def compare_with_mongodb_example():
    """Compare with the MongoDB example data"""
    print(f"\nComparing with MongoDB Example")
    print("=" * 50)
    
    week_id = "2025-W27"
    week_start, week_end = get_week_date_range(week_id)
    
    print(f"MongoDB Week: {week_id}")
    print(f"  Week Range: {week_start.strftime('%Y-%m-%d')} ({week_start.strftime('%A')}) to {week_end.strftime('%Y-%m-%d')} ({week_end.strftime('%A')})")
    
    print(f"This should be Monday-Sunday (ISO week)")

if __name__ == "__main__":
    test_week_boundaries()
    compare_with_mongodb_example() 