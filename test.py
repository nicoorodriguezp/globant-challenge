import unittest
import requests

class TestAPI(unittest.TestCase):

    def test_healthcheck_endpoint(self):
        url = 'http://localhost:5000/healthcheck/'
        response = requests.get(url)
        self.assertEqual(response.status_code, 200)
        print('Health Check --> OK.')


if __name__ == '__main__':
    suite = unittest.TestSuite()
    suite.addTest(TestAPI('test_healthcheck_endpoint'))

    unittest.TextTestRunner(verbosity=2).run(suite)
