#!/usr/bin/env python3

import calendar
from datetime import date
from datetime import timedelta
from dateutil import rrule


start_day = date(2017, 6, 17)
# end_day = date(2022, 11, 12)
end_day = date(2022, 11, 17)


def iterate_months(start, end):
    """Iterates over months in the supplied date range, returning tuples of dates
    representing each month's start and end.

    """
    if end < start:
        raise ValueError(f"End cannot be earlier than start: {end} < {start}.")

    def month_date_range(dt):
        _, mr_end = calendar.monthrange(dt.year, dt.month)
        mstart = max(start, date(dt.year, dt.month, 1))
        mend = min(end, date(dt.year, dt.month, mr_end))
        return mstart, mend

    for dt in rrule.rrule(rrule.MONTHLY, dtstart=start, until=end):
        mstart, mend = month_date_range(dt)
        yield mstart, mend

    if mend < end_day:
        yield month_date_range(mend + timedelta(days=1))


for start, end in iterate_months(start_day, end_day):
    print(start, end)
