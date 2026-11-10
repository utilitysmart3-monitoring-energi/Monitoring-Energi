import unittest
from unittest.mock import patch

from engine import history


class Response:
    def __init__(self, registers):
        self.registers = registers

    def isError(self):
        return False


class FakeClient:
    def __init__(self):
        self.calls = []

    def read_holding_registers(self, *, address, count, slave):
        self.calls.append((address, count, slave))
        if address == 2999:
            registers = [0x3F80, 0, 0x4000, 0, 0x4040, 0]
        else:
            registers = [0] * count
        return Response(registers)


class ModbusReadTests(unittest.TestCase):
    def test_all_reads_use_pymodbus_38_slave_argument(self):
        client = FakeClient()

        with patch.object(history.time, "sleep", return_value=None):
            data = history.get_full_data(client, 6)

        self.assertIsNotNone(data)
        self.assertEqual(len(client.calls), 14)
        self.assertTrue(all(slave_id == 6 for _, _, slave_id in client.calls))
        self.assertEqual(data["current_i1"], 1.0)
        self.assertEqual(data["current_i2"], 2.0)
        self.assertEqual(data["current_i3"], 3.0)


if __name__ == "__main__":
    unittest.main()