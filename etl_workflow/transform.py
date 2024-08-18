class Transformer:
    def __init__(self):
        pass

    def transform_rows(self, rows):
        transformed_rows = []
        for row in rows:
            try:
                # Row tuple: (id, category, name, quantity, amount_excl_tax, amount_inc_tax, transaction_date)
                id_, category, name, quantity, amount_excl_tax, amount_inc_tax, transaction_date = row

                # Transformations
                id_ = str(id_).strip()  # Ensure ID is a string and trim whitespace
                category = category.strip().upper()  # Normalize category, trim and title case
                name = name.strip()  # Trim whitespace from name
                quantity = int(quantity)  # Ensure quantity is an integer
                amount_excl_tax = round(float(amount_excl_tax), 2)  # Convert to float and round to 2 decimal places
                amount_inc_tax = round(float(amount_inc_tax), 2)  # Convert to float and round to 2 decimal places
                transaction_date = transaction_date # Normalization done before when extracted from csv

                # Check if amount_inc_tax is correct
                self.check_20_percent_tax(amount_excl_tax, amount_inc_tax)

                # Add the transformed row to the list
                transformed_rows.append((id_, category, name, quantity, amount_excl_tax, amount_inc_tax, transaction_date))
            
            except (ValueError, TypeError, AssertionError) as e:
                print(f"Transformation error: {e}")
                continue  # Skip this row if any transformation fails
        
        return transformed_rows

    def check_20_percent_tax(self, amount_excl_tax, amount_inc_tax):
        # Calculate the expected amount with 20% tax
        expected_amount_inc_tax = round(amount_excl_tax * 1.2, 2)

        # Check if the calculated amount with tax matches the provided amount_inc_tax
        if expected_amount_inc_tax != amount_inc_tax:
            raise ValueError(f"Incorrect tax amount: expected {expected_amount_inc_tax}, got {amount_inc_tax}")