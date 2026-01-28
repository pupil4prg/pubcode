
import sys
import unittest
from unittest.mock import MagicMock

# Mock dataiku module before importing osvrapper
sys.modules['dataiku'] = MagicMock()

# Now we can import the module to check for syntax errors and test logic
import osvrapper

class TestOSWrapper(unittest.TestCase):
    def test_list_files_regex(self):
        folder = MagicMock()
        folder.list_paths_in_partition.return_value = ['/data/a.txt', '/data/b.csv', '/other/c.txt']
        
        # Test regex
        result = osvrapper.list_files(folder, path=r'.*\.csv$', is_regex=True)
        self.assertEqual(result, ['/data/b.csv'])
        
    def test_list_files_prefix(self):
        folder = MagicMock()
        folder.list_paths_in_partition.return_value = ['/data/a.txt', '/data/b.csv', '/other/c.txt']
        
        # Test prefix
        result = osvrapper.list_files(folder, path='/data', is_regex=False)
        self.assertEqual(result, ['/data/a.txt', '/data/b.csv'])

if __name__ == '__main__':
    unittest.main()
