import unittest
from transform import Transformer

class TestTransformRows(unittest.TestCase):

    def setUp(self):
        self.transformer = Transformer()

    def test_transform_rows(self):
        # Sample input
        input_rows = [
            ('1a-a','Buy','Addidas Running Shoes    ',5,399.95,479.94,'2022-01-15'),
            ('2b-b','buy','Fitbit Charge 5','5',449.95,539.94, '2022-01-15'),
            ('3c-c','buY','Salomon Jacket',5,799.95,959.94, '2022-01-15')
        ]

        # Expected output
        expected_rows = [
            ('1a-a','BUY','Addidas Running Shoes',5,399.95,479.94,'2022-01-15'),
            ('2b-b','BUY','Fitbit Charge 5',5,449.95,539.94, '2022-01-15'),
            ('3c-c','BUY','Salomon Jacket',5,799.95,959.94, '2022-01-15')
        ]

        # Call the transform method of Transformer class
        transformed_rows = self.transformer.transform_rows(input_rows)

        # Assert the transformation matches the expected output
        self.assertEqual(transformed_rows, expected_rows)

    def test_invalid_data(self):
        # Test handling of invalid data
        input_rows = [
            ('1a-a','Buy','Addidas Running Shoes    ',5,399.95,479.94,'2022-01-15'),
            ('2b-b','buy','Fitbit Charge 5','5',449.95,539.94, '2022-01-15'),
            ('4d-d','BUY','Addidas Running Shoes','abc',399.95,479.94,'2022-01-15'),
            ('5e-e','BUY','Fitbit Charge 5',5,449.95,'539.94€', '2022-01-15'),
            ('6f-f','BUY','Fitbit Charge 5',5,449.95,600.94, '2022-01-15'),
            ('3c-c','buY','Salomon Jacket',5,799.95,959.94, '2022-01-15')
        ]

        expected_rows = [
            ('1a-a','BUY','Addidas Running Shoes',5,399.95,479.94,'2022-01-15'),
            ('2b-b','BUY','Fitbit Charge 5',5,449.95,539.94, '2022-01-15'),
            ('3c-c','BUY','Salomon Jacket',5,799.95,959.94, '2022-01-15')
        ]

        # Call the transform method of Transformer class
        transformed_rows = self.transformer.transform_rows(input_rows)

        # Assert the transformation matches the expected output
        self.assertEqual(transformed_rows, expected_rows)

if __name__ == '__main__':
    unittest.main()
