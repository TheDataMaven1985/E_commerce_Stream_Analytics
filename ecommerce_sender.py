import socket
import time
import random
import json
from datetime import datetime

# Define data configuration PRODUCTS, LOCATIONS, PAYMENT_METHODS
PRODUCTS = {
    'ELECTRONICS': [
        {'id': 'ELEC001', 'name': 'Television', 'price': 200},
        {'id': 'ELEC002', 'name': 'WirelessHeadphone', 'price': 55.50},
        {'id': 'ELEC003', 'name': 'Smart Watch', 'price': 85.99},
        {'id': 'ELEC004', 'name': 'Bluetooth Speaker', 'price': 64.99}
    ],
    'CLOTHING': [
        {'id': 'CLTH001', 'name': 'Sneakers', 'price': 45.99},
        {'id': 'CLTH002', 'name': 'Winter Jacket', 'price': 13.99},
        {'id': 'CLTH003', 'name': 'Jeans', 'price': 49.99},
        {'id': 'CLTH004', 'name': 'T-Shirt', 'price': 12.99}
    ],
    'HOME': [
        {'id': 'HOME001', 'name': 'Coffee Maker', 'price': 105.99},
        {'id': 'HOME002', 'name': 'vaccum cleaner', 'price': 95.99},
        {'id': 'HOME003', 'name': 'Bed Cover', 'price': 82.99}
    ],
    'BOOKS': [
        {'id': 'BOOK001', 'name': 'Born Rich', 'price': 42.29},
        {'id': 'BOOK002', 'name': 'Sweet Sixteen', 'price': 10.99},
        {'id': 'BOOK003', 'name': 'Road to Paradise', 'price': 11.99},
        {'id': 'BOOK004', 'name': 'Red Hand-Man', 'price': 22.99}
    ]
}

LOCATIONS = {
    'USA': ['New York', 'Los Angeles', 'Chicago', 'Houston', 'Phoenix'],
    'UK': ['London', 'Manchester', 'Birmingham', 'Leeds'],
    'Canada': ['Toronto', 'Vancouver', 'Montreal'],
    'Germany': ['Berlin', 'Munich', 'Hamburg'],
    'Australia': ['Sydney', 'Melbourne', 'Brisbane']
}

PAYMENT_METHODS = ['Credit Card', 'PayPal', 'Debit Card', 'Apple Pay', 'Google Pay']

# Function to generate transaction data
def generate_transaction():
    transaction_id = f"TXN{int(time.time())}{random.randint(100, 999)}"
    customer_id = f"CUST{random.randint(1, 500):04d}"
    category = random.choice(list(PRODUCTS.keys()))
    product = random.choice(PRODUCTS[category])
    quantity = random.choices([1, 2, 3, 4], weights=[70, 20, 3, 7])[0]
    total_amount = round(quantity * product['price'], 2)
    country = random.choice(list(LOCATIONS.keys()))
    city = random.choice(LOCATIONS[country])

    transaction = {
        'transaction_id': transaction_id,
        'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'customer_id': customer_id,
        'product_id': product['id'],
        'product_category': category,
        'product_name': product['name'],
        'quantity': quantity,
        'unit_price': product['price'],
        'total_amount': total_amount,
        'payment_method': random.choice(PAYMENT_METHODS),
        'country': country,
        'city': city
    }

    return transaction

# Create socket server to send data
def streaming():
    #  socket config
    HOST = 'localhost'
    PORT = 9999

    socket_server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    socket_server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    socket_server.bind((HOST, PORT))
    socket_server.listen(1)

    conn, addr = socket_server.accept()
    print(f"Connected! Client address: {addr}")

    print("Sending transactions...\n")
    # transactions stats
    transaction_count = 0
    total_revenue = 0

    try:
        while True:
            transaction = generate_transaction()

            transaction_count +=1
            total_revenue += transaction['total_amount']

            message = json.dumps(transaction) + '\n'        # converts dic to json and new line
            conn.send(message.encode('utf-8'))              # Send the message through the socket (encode to bytes)

            if transaction_count % 10 == 0:                 # Prints every transaction to avoid cluster
                print(f"[{transaction_count}] Sent: {transaction['product_name']}"
                      f"(${transaction['total_amount']}) to {transaction['city']}, {transaction['country']}")
                print(f" Total Revenue: ${total_revenue:,.2f}")

            # Wait before sending next transaction
            time.sleep(random.uniform(0.3, 1.5))            # Random sleep between 0.3 and 1.5 seconds

    except BrokenPipeError:
        print("Connection lost! The receiver disconnected.")
        print(f"Sent {transaction_count} transactions before disconnect.")
    
    finally:
        conn.close
        socket_server.close()
        print("Server shut down...")

if __name__ == "__main__":
    print("STARTING E-COMMERCE TRANSACTION STREAM")
    print("\nPress Ctrl+C to stop the stream")

    streaming()


