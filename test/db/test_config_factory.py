import unittest
from graph_cast.db.factory import ConfigFactory


class FactoryTest(unittest.TestCase):
    arango_args = {
        "protocol": "http",
        "ip_addr": "127.0.0.1",
        "port": 8529,
        "cred_name": "root",
        "cred_pass": "123",
        "database": "root",
        "db_type": "arango",
    }

    def test_factory(self):
        ac = ConfigFactory.create_config(args=self.arango_args)
        self.assertEqual(ac.port, 8529)


if __name__ == "__main__":
    unittest.main()
