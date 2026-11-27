import os
import unittest
from datetime import date


@unittest.skipUnless(os.getenv("DATABASE_URL"), "DATABASE_URL is not configured")
class TestAnalytics(unittest.TestCase):
    def test_optional_filters_return_warehouse_metrics(self):
        from main import analytics

        unfiltered = analytics()
        filtered = analytics(
            category="Electronics",
            region="North",
            start_date=date(2026, 1, 1),
            end_date=date(2026, 12, 31),
        )

        self.assertGreater(unfiltered["metrics"]["records"], 0)
        self.assertGreater(filtered["metrics"]["records"], 0)
        self.assertLessEqual(filtered["metrics"]["records"], unfiltered["metrics"]["records"])
        self.assertEqual({row["region"] for row in filtered["regions"]}, {"North"})
        self.assertEqual({row["category"] for row in filtered["categories"]}, {"Electronics"})
