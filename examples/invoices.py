"""A small handler module for trying `jobqueue work --handlers`.

From the examples directory:

    jobqueue --db demo.db enqueue invoice.render --payload '{"number": 1042, "total": 99.5}'
    jobqueue --db demo.db enqueue invoice.render --payload '{"number": 1043}'
    jobqueue --db demo.db work --handlers invoices:registry --burst
    jobqueue --db demo.db dead
"""

from jobqueue import Abandon, ExponentialBackoff, HandlerRegistry, Retry

registry = HandlerRegistry()


@registry.handler(
    "invoice.render",
    max_attempts=3,
    backoff=ExponentialBackoff(base=2.0, factor=2.0, maximum=30.0),
)
def render_invoice(payload):
    # A malformed invoice will not get better by trying again.
    if "total" not in payload:
        raise Abandon(f"invoice {payload.get('number')} has no total")
    return {"file": f"invoice-{payload['number']}.txt", "total": payload["total"]}


@registry.handler("invoice.send")
def send_invoice(payload, ctx):
    # The fictional mail service is down on the first try, and asks us to come back.
    if ctx.attempt == 1:
        raise Retry(delay=1.0, reason="mail service answered 503")
    return {"sent_to": payload["to"], "attempt": ctx.attempt}
