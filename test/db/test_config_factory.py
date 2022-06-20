import unittest
from graph_cast.db.factory import ConfigFactory


class FactoryTest(unittest.TestCase):

    def test_factory(self):
        arango_args = {
            "protocol": "http",
            "ip_addr": "127.0.0.1",
            "port": 8529,
            "cred_name": "root",
            "cred_pass": "123",
            "database": "root",
            "db_type": "arango",
        }
        ac = ConfigFactory.create_config(args=arango_args)
        self.assertEqual(ac.port, 8529)

    def test_wsgi(self):
        args = {
            "db_type": "wsgi",
            "protocol": "http",
            "ip_addr": "127.0.0.1",
            "port": 8529,
            "path": "/re"
        }
        ac = ConfigFactory.create_config(args=args)
        self.assertEqual(ac.hosts[-2:], "re")


if __name__ == "__main__":
    unittest.main()
