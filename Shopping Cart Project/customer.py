# customer.py

class Customer:
    def __init__(self):
        self.name = ""
        self.address = ""
        self.distance_km = 0

    def get_details(self):
        """Take customer details"""
        self.name = input("Enter your name: ")
        self.address = input("Enter your address: ")
        self.distance_km = int(input("Enter distance from store (in km): "))

    def calculate_delivery_charges(self):
        """Calculate delivery charges based on distance"""
        if self.distance_km <= 15:
            return 50
        elif self.distance_km <= 30:
            return 100
        else:
            return None
