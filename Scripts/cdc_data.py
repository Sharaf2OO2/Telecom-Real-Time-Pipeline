from faker import Faker
from datetime import datetime, timedelta
import random as r, psycopg2

# Database connection settings
DB_NAME = "telecom_db"
DB_USER = "admin"
DB_PASS = "admin"
DB_HOST = "localhost"
DB_PORT = "5432"

fake = Faker()
regions = ['Cairo', 'Giza', 'Alexandria', 'Aswan', 'Luxor', 'Qena', 'Assiut', "Sohag"]

NUM_SUBSCRIBERS = 500
NUM_BILLING_EVENTS = 1000
NUM_TOWERS = 20

def create_connection():
    return psycopg2.connect(
        dbname=DB_NAME, user=DB_USER, password=DB_PASS,
        host=DB_HOST, port=DB_PORT
    )

def generate_subscribers(cur):
    for _ in range(NUM_SUBSCRIBERS):
        name = fake.name()
        phone = f"+20{r.choice(['10', '11', '12'])}{r.randint(10000000, 99999999)}"
        plan = r.choice(["Prepaid", "Postpaid"])
        region = r.choice(regions)
        join_date = fake.date_between(start_date="-3y", end_date="today")
        status = r.choice(["active", "inactive", "suspended"])
        cur.execute(
            """
            INSERT INTO subscribers(name, phone_number, plan_type, region, join_date, status)
            VALUES(%s, %s, %s, %s, %s, %s)
            """,
            (name, phone, plan, region, join_date, status)
        )

def generate_cell_towers(cur):
    for region in regions:
        for i in range(NUM_TOWERS):
            location = f"{region} Area {i+1}"
            capacity = r.randint(300, 600)
            cur.execute(
                """
                INSERT INTO cell_towers (location, capacity, region)
                VALUES (%s, %s, %s)
                """,
            (location, capacity, region)
            )

def generate_billing_events(cur):
    # Get subscriber IDs for foreign key reference
    cur.execute("SELECT subscriber_id FROM subscribers")
    subscriber_ids = [row[0] for row in cur.fetchall()]

    for _ in range(NUM_BILLING_EVENTS):
        subscriber_id = r.choice(subscriber_ids)
        amount = round(r.uniform(50, 400), 2)
        bill_date = datetime.utcnow() - timedelta(days=r.randint(0, 365))
        status = r.choice(["paid", "unpaid"])
        cur.execute(
            """
            INSERT INTO billing_events (subscriber_id, amount, bill_date, status)
            VALUES (%s, %s, %s, %s)
            """,
            (subscriber_id, amount, bill_date, status)
        )


if __name__ == "__main__":
    conn = create_connection()
    conn.autocommit = True
    cur = conn.cursor()

    print("Generating subscribers...")
    generate_subscribers(cur)
    print("Generating cell towers...")
    generate_cell_towers(cur)
    print("Generating billing events...")
    generate_billing_events(cur)

    cur.close()
    conn.close()
    print("Data generation completed.")
    