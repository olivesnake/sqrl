import unittest
import sqrl.core as core


class GetTableInfo(unittest.TestCase):
    def test_get_table_names(self):
        db = core.SQL("../chinook.db")
        result = db.get_table_names()
        result.sort()
        self.assertEqual(result,
                         ['albums', 'artists',
                          'customers',
                          'employees',
                          'genres',
                          'invoice_items',
                          'invoices',
                          'media_types',
                          'playlist_track',
                          'playlists',
                          'sqlite_sequence',
                          'sqlite_stat1',
                          'tracks']
                         )

    def test_get_column_names(self):
        db = core.SQL("../chinook.db")
        result = db.get_column_names("albums")
        self.assertIsNotNone(result)
        self.assertEqual(result, ["AlbumId", "Title", "ArtistId"])

    def test_get_column_names_nonexistent(self):
        db = core.SQL("../chinook.db")
        result = db.get_column_names("unreal")
        self.assertFalse(result)


class Select(unittest.TestCase):
    def test_normal(self):
        pass

    def test_limit_1(self):
        db = core.SQL("../chinook.db")
        result = db.fetch("select name from artists limit 1;")
        self.assertIsInstance(result, str)
        self.assertEqual(result, "AC/DC")
        result2 = db.fetch("select name from artists limit 10 offset 10;")
        self.assertEqual(result2, ['Black Label Society', 'Black Sabbath', 'Body Count', 'Bruce Dickinson', 'Buddy Guy',
                                   'Caetano Veloso', 'Chico Buarque', 'Chico Science & Nação Zumbi', 'Cidade Negra',
                                   'Cláudio Zoli'])


class CreateTableFromJSON(unittest.TestCase):
    def test_invalid_json_file(self):
        db = core.SQL()
        self.assertFalse(db.create_table_from_json("bad.json"))

    def test_existing_table_name(self):
        db = core.SQL()
        self.assertTrue(db.execute("CREATE TABLE artists (foo integer, bar text)"))
        self.assertFalse(db.create_table_from_json("artists.json"))

    def test_normal(self):
        db = core.SQL()
        self.assertTrue(db.create_table_from_json("artists.json"))

        ans = [{'ArtistId': 43, 'Name': 'A Cor Do Som'}, {'ArtistId': 1, 'Name': 'AC/DC'},
               {'ArtistId': 230, 'Name': 'Aaron Copland & London Symphony Orchestra'},
               {'ArtistId': 202, 'Name': 'Aaron Goldberg'},
               {'ArtistId': 214, 'Name': 'Academy of St. Martin in the Fields & Sir Neville Marriner'}]

        self.assertEqual(ans, db.select("artists", return_as_dict=True, limit=5, order_by='Name'))


class CreateTableFromCsv(unittest.TestCase):
    def test_normal(self):
        db = core.SQL()
        self.assertTrue(db.create_table_from_csv("chinook-artists.csv"))
        ans = [{'ArtistId': 43, 'Name': 'A Cor Do Som'}, {'ArtistId': 1, 'Name': 'AC/DC'},
               {'ArtistId': 230, 'Name': 'Aaron Copland & London Symphony Orchestra'},
               {'ArtistId': 202, 'Name': 'Aaron Goldberg'},
               {'ArtistId': 214, 'Name': 'Academy of St. Martin in the Fields & Sir Neville Marriner'}]

        self.assertEqual(ans, db.select("chinook_artists", return_as_dict=True, limit=5, order_by='Name'))


if __name__ == '__main__':
    unittest.main()
