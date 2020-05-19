import pytest
from _pytest.monkeypatch import MonkeyPatch

import sync


class Struct:
    def __init__(self, **entries):
        self.__dict__.update(entries)


class Test():
    monkeypatch = MonkeyPatch()
    user_ids = []
    key_values = []

    # Mock what the database would return for a query.
    def mock_get_rows(self, args):
        return [
            {'user_id': 1, 'columntest': 'asdf'},
            {'user_id': 2, 'columntest': 'qwerty', 'extra': 'nothing'}
        ]

    # Mock calling ActionKit, record what would be sent.
    def mock_set_usertag(self, user_id, key_values):
        self.user_ids.append(user_id)
        self.key_values.append(key_values)

    def test_sync(self):
        Test.monkeypatch.setattr("sync.get_rows", self.mock_get_rows)
        Test.monkeypatch.setattr("actionkit.api.user.AKUserAPI.set_usertag", self.mock_set_usertag)
        # All the connection and SQL values are being mocked. Only SQL_COLUMNS
        # and AK_USERFIELDS are actually used in tests.
        args = {
            'AK_BASEURL': 'mock',
            'AK_USER': 'mock',
            'AK_PASSWORD': 'mock',
            'DB_HOST': 'mock',
            'DB_PORT': 'mock',
            'DB_USER': 'mock',
            'DB_PASS': 'mock',
            'DB_NAME': 'mock',
            'SQL_FILE': 'mock',
            'SQL_LIMIT': 'mock',
            'SQL_COLUMNS': 'columntest',
            'AK_USERFIELDS': 'fieldtest'
        }
        args = Struct(**args)
        sync.sync(args)
        assert self.user_ids == [1, 2]
        assert self.key_values == [{'fieldtest': 'asdf'}, {'fieldtest': 'qwerty'}]
