# store.py

class ShoppingCart:
    def __init__(self):
        # Dictionary: item: [price, quantity]
        self.items = {
            "Apple": [20, 10],
            "Mango": [30, 5],
            "Orange": [40, 8],
            "Banana": [50, 12],
            "Pineapple": [40, 3]
        }
        self.cart = {}

    def menu(self):
        """Display store menu with prices and stock"""
        print("\n--- Store Menu ---")
        print("Item\t\tPrice\tStock")
        print("-" * 30)
        for item, details in self.items.items():
            print(f"{item}\t\tRs {details[0]}\t{details[1]} available")

    def select_item(self):
        """Allow multiple item selections until customer is done"""
        while True:
            item_name = input("\nEnter the item you want to buy (or type 'done' to finish): ").title()
            if item_name.lower() == "done":
                break

            if item_name not in self.items:
                print("❌ Item not found!")
                continue

            qty = int(input(f"Enter quantity of {item_name}: "))

            if qty > self.items[item_name][1]:
                print("❌ Sorry, not enough stock!")
            else:
                self.cart[item_name] = self.cart.get(item_name, 0) + qty
                self.items[item_name][1] -= qty
                print(f"✅ {qty} {item_name}(s) added to your cart.")
