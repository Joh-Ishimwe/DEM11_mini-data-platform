# Sample data

**Committed on purpose.** `data/raw/` is gitignored; this folder is not.

CI runners are blank machines with no access to production, so your tests are
only as good as the fixtures sitting here. A sample with three clean rows
makes a green tick that proves nothing.

Regenerate them any time with:

    make seed

A good sample deliberately includes a null where a value is expected, a
duplicate key, a price with a currency symbol, an unparseable date, and
inconsistent casing.

**Never commit real customer data here.** Synthesise it.
