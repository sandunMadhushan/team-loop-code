import pandas as pd
import os

def load_csv_data(data_dir):
    """
    Loads the product and customer CSV files into pandas DataFrames.

    Args:
        data_dir (str): The path to the input data directory.

    Returns:
        tuple: A tuple containing the products DataFrame and customers DataFrame.
    """
    products_path = os.path.join(data_dir, 'products_list.csv')
    customers_path = os.path.join(data_dir, 'customer_data.csv')

    products_df = pd.read_csv(products_path)
    customers_df = pd.read_csv(customers_path)

    print("Successfully loaded products and customer data.")
    return products_df, customers_df

if __name__ == '__main__':
    # This is for testing the module directly.
    # The script will be run from evidence/executables, so the path needs to be relative.
    # To run this directly from src/, we need to adjust the path for testing.
    # This assumes we are running from the 'src' directory for a direct test.
    if os.getcwd().endswith('src'):
        test_data_dir = '../../data/input'
    else: # Assuming running from project root for other cases
        test_data_dir = '../data/input'

    if not os.path.exists(test_data_dir):
         # Path for when run from `evidence/executables`
        test_data_dir = '../../../data/input'

    if os.path.exists(test_data_dir):
        products, customers = load_csv_data(test_data_dir)
        print("\nProducts DataFrame:")
        print(products.head())
        print("\nCustomers DataFrame:")
        print(customers.head())
    else:
        print(f"Error: Could not find the data directory at {test_data_dir}")
        print(f"Current working directory: {os.getcwd()}")