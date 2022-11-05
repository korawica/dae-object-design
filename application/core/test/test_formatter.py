import unittest
import application.core.formatter as formatter


class DatetimeTest(unittest.TestCase):

    def setUp(self) -> None:
        self.dt = formatter.Datetime.parse('2021-01-1 135043', '%Y-%m-%-d %f')
        self.dt2 = formatter.Datetime({
            'year': '2022',
            'weeks_year_sun_pad': '02',
            'week': '6',
        })

    def test_datetime_parser(self):
        self.assertEqual('2021-01-01 00:00:00.135', self.dt.standard_value)
        self.assertEqual('2022-01-15 00:00:00.000', self.dt2.standard_value)

    def test_datetime_order(self):
        self.assertTrue(
            formatter.Datetime.parse('2021-01-1 135043', '%Y-%m-%-d %f') <=
            formatter.Datetime.parse('2021-01-2 135043', '%Y-%m-%-d %f')
        )

    def test_level_compare(self):
        self.assertListEqual([True, False, False, False, False, True, True, True], self.dt.level.slot)
        self.assertEqual(22, self.dt.level.value)
        self.assertListEqual([False, False, False, False, False, True, True, True], self.dt2.level.slot)
        self.assertEqual(21, self.dt2.level.value)


class VersionTest(unittest.TestCase):

    def test_version_parser(self):
        self.assertEqual('v0.0.0+asdf_asdf.sadf', formatter.Version.parse('+asdf_asdf.sadf', '%l').standard_value)


class SerialTest(unittest.TestCase):

    def setUp(self) -> None:
        self.sr: formatter.Formatter = formatter.Serial.parse('009', '%p')
        self.sr2: formatter.Formatter = formatter.Serial.parse('00001101', '%b')
        self.sr3: formatter.Formatter = formatter.Serial()

    def test_serial_parser(self):
        self.assertEqual(9, self.sr.standard)
        self.assertEqual('9', self.sr.standard_value)
        self.assertEqual('00001001', self.sr.format('%b'))
        self.assertEqual('009', self.sr.format('%p'))
        self.assertEqual(13, self.sr2.standard)

    def test_serial_order(self):
        self.assertTrue(self.sr <= self.sr2)
        self.assertTrue(self.sr < self.sr2)
        self.assertFalse(self.sr == self.sr2)
        self.assertFalse(self.sr >= self.sr2)
        self.assertFalse(self.sr > self.sr2)

    def test_level_compare(self):
        self.assertEqual(1, self.sr.level.value)
        self.assertEqual(0, self.sr3.level.value)
        self.assertTrue(self.sr.level == self.sr2.level)
        self.assertFalse(self.sr3.level == self.sr2.level)
        self.assertTrue(self.sr3.level < self.sr2.level)


class OrderFormatTest(unittest.TestCase):

    def test_order_timestamp(self):
        self.assertTrue(
            formatter.OrderFormat({'timestamp': {'value': '20220101', 'fmt': '%Y%m%d'}}) ==
            formatter.OrderFormat({'timestamp': {'value': '20220101', 'fmt': '%Y%m%d'}})
        )
        self.assertTrue(
            formatter.OrderFormat({'timestamp': {'value': '01', 'fmt': '%d'}}) <=
            formatter.OrderFormat({'timestamp': {'value': '01', 'fmt': '%d'}})
        )
        self.assertTrue(
            formatter.OrderFormat({'timestamp': formatter.Datetime.parse('20220101', '%Y%m%d')}) <
            formatter.OrderFormat({'timestamp': formatter.Datetime.parse('20220102', '%Y%m%d')})
        )

    def test_order_version(self):
        self.assertFalse(
            formatter.OrderFormat({'version': formatter.Version.parse('001', '%m%n%c')}) ==
            formatter.OrderFormat({'version': formatter.Version.parse('002', '%m%n%c')})
        )

    def test_order_serial(self):
        self.assertTrue(
            formatter.OrderFormat({'serial': {'fmt': '%n', 'value': '2'}}) >=
            formatter.OrderFormat({'serial': {'fmt': '%n', 'value': '1'}})
        )

    def test_order_version_adjust(self):
        self.assertTrue(
            formatter.OrderFormat({
                'timestamp': formatter.Datetime.parse('20220102', '%Y%m%d'),
                'version': formatter.Version.parse('127', '%m%n%c')
            }).adjust_version('*.1.3') <=
            formatter.OrderFormat({
                'version': formatter.Version.parse('114', '%m%n%c')
            })
        )
