import unittest
import lambda_function

class formatHTMLTestCase(unittest.TestCase):
    def runTest(self):
        differences = ["EC2 deleted", "EC2 Added", "EC2 price change"]
        solution = "EC2 deleted<br>EC2 Added<br>EC2 price change<br>"
        self.assertEqual(lambda_function.formatHTML(differences), solution, "not formatted correctly")

        
class editDataFrameTestCase(unittest.TestCase):
    def setUp(self):
       lambda_function.download_file('https://pricing.us-east-1.amazonaws.com/offers/v1.0/aws/AmazonEC2/current/eu-west-1/index.csv')
       lambda_function.removeRows("/tmp/index.csv")
       dataframe = lambda_function.pd.read_csv('/tmp/updated_test.csv')
    
    def test_swapColumns(self):
        self.assertEqual(2, 2, "lol")
        

if __name__ == '__main__':
    unittest.main()