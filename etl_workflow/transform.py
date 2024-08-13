from datetime import datetime

def transform_rows(rows):
    transformed_rows = []
    for row in rows:
        try:
            # Assuming the row is a tuple (id, category, name, quantity, amount_excl_tax, amount_inc_tax, transaction_date)
            id_, category, name, quantity, amount_excl_tax, amount_inc_tax, transaction_date = row

            # Transformations
            id_ = str(id_).strip()  # Ensure ID is a string and trim whitespace
            category = category.strip().upper()  # Normalize category, trim and title case
            name = name.strip()  # Trim whitespace from name
            quantity = int(quantity)  # Ensure quantity is an integer
            amount_excl_tax = round(float(amount_excl_tax), 2)  # Convert to float and round to 2 decimal places
            amount_inc_tax = round(float(amount_inc_tax), 2)  # Convert to float and round to 2 decimal places
            transaction_date = transaction_date # Normalization done before when extracted from csv

            # Add the transformed row to the list
            transformed_rows.append((id_, category, name, quantity, amount_excl_tax, amount_inc_tax, transaction_date))
        
        except (ValueError, TypeError, AssertionError) as e:
            print(f"Transformation error: {e}")
            continue  # Skip this row if any transformation fails
    
    return transformed_rows