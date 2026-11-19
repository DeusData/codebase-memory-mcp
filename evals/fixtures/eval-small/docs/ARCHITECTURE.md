# Architecture

## Orders

`OrderService.checkout` validates an order with `validate` and renders the
receipt with `notify`, which formats money through `format_cents`.

## Billing

`InvoiceService.issue` audits an invoice and formats the total with
`Money.format`.

## Dead code

`unused_helper` is intentionally unreferenced.
