import unittest
import os
import json
from unittest.mock import patch, mock_open
from llm.dags.validate import ValidateDocument

class TestValidateDocument(unittest.TestCase):

    def setUp(self):
        self.validator = ValidateDocument()
        self.validator.initialize('/fake/path', 'info.txt', ['key1', 'key2'])

    @patch('builtins.open', new_callable=mock_open, read_data='{"key1": "value1", "key2": "value2"}')
    def test_validate_required_info_txt_all_keys_present(self, mock_file):
        result, message, file_name = self.validator.validate_required_info_txt()
        self.assertTrue(result)
        self.assertEqual(message, "")
        self.assertEqual(file_name, 'info.txt')

    @patch('builtins.open', new_callable=mock_open, read_data='{"key1": "value1"}')
    def test_validate_required_info_txt_missing_key(self, mock_file):
        result, message, file_name = self.validator.validate_required_info_txt()
        self.assertFalse(result)
        self.assertIn('Key key2 is missing', message)
        self.assertEqual(file_name, 'info.txt')

    @patch('builtins.open', new_callable=mock_open, read_data='{}')
    def test_validate_required_info_txt_all_keys_missing(self, mock_file):
        result, message, file_name = self.validator.validate_required_info_txt()
        self.assertFalse(result)
        self.assertIn('Key key1 is missing', message)
        self.assertEqual(file_name, 'info.txt')

    @patch('builtins.open', new_callable=mock_open, read_data='not a json')
    def test_validate_required_info_txt_invalid_json(self, mock_file):
        with self.assertRaises(json.JSONDecodeError):
            self.validator.validate_required_info_txt()

if __name__ == '__main__':
    unittest.main()