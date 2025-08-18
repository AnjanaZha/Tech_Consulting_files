# main.py
from store_menu import ShoppingCart
from customer import Customer

# Step 1: Create objects
shop = ShoppingCart()
customer = Customer()

# Step 2: Show menu & let customer select items
shop.menu()
shop.select_item()

# Step 3: Take customer details
customer.get_details()

# Step 4: Calculate bill
delivery_charges = customer.calculate_delivery_charges()

if delivery_charges is None:
    print("❌ Delivery not available for your location.")
else:
    total = sum(shop.items[item][0] * qty for item, qty in shop.cart.items())
    total_with_delivery = total + delivery_charges

    # Step 5: Display final bill
    print("\n--- Final Bill ---")
    print(f"Customer: {customer.name}")
    print(f"Address: {customer.address}")
    print(f"Distance: {customer.distance_km} km")
    print("\nItems Purchased:")
    for item, qty in shop.cart.items():
        print(f"{item} - {qty} pcs - Rs {shop.items[item][0]} each")
    print(f"Delivery Charges: Rs {delivery_charges}")
    print(f"Total Amount: Rs {total_with_delivery}")
